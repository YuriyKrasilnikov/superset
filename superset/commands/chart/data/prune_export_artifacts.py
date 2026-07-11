# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""Lifecycle command for expired chart-data export artifacts."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from flask import current_app
from superset_core.tasks.types import TaskStatus

from superset import db
from superset.charts.data.artifacts import (
    delete_chart_data_export_artifact_transactionally,
)
from superset.commands.base import BaseCommand
from superset.commands.tasks.internal_update import InternalStatusTransitionCommand
from superset.daos.chart_data_export_artifact import ChartDataExportArtifactDAO
from superset.tasks.constants import ACTIVE_STATES
from superset.tasks.manager import TaskManager

if TYPE_CHECKING:
    from superset.models.tasks import Task

logger = logging.getLogger(__name__)


def _fail_expired_artifact_task(task: Task | None) -> bool:
    """Terminalize an active task whose artifact generation deadline expired."""
    if task is None or task.status not in ACTIVE_STATES:
        return False

    properties = {
        **task.properties_dict,
        "error_message": "Chart-data export expired before generation completed",
        "exception_type": "ArtifactGenerationExpired",
    }
    transitioned = InternalStatusTransitionCommand(
        task_uuid=task.uuid,
        new_status=TaskStatus.FAILURE,
        expected_status=[
            TaskStatus.PENDING,
            TaskStatus.IN_PROGRESS,
            TaskStatus.ABORTING,
        ],
        properties=properties,
        set_ended_at=True,
    ).run()
    if transitioned:
        try:
            TaskManager.publish_completion(task.uuid, TaskStatus.FAILURE.value)
        except Exception:  # pylint: disable=broad-except
            logger.exception(
                "Failed to publish expiration failure for chart-data export task %s",
                task.uuid,
            )
    return transitioned


class ChartDataExportArtifactPruneCommand(BaseCommand):
    """Remove expired export objects without orphaning their storage keys."""

    def __init__(self, max_rows_per_run: int | None = None) -> None:
        self._max_rows_per_run = max_rows_per_run

    def run(self) -> int:
        """Delete expired artifacts independently and return the deleted count."""
        expires_before = datetime.now(timezone.utc).replace(tzinfo=None)
        artifacts = ChartDataExportArtifactDAO.find_expired(
            expires_before,
            self._max_rows_per_run,
        )
        deleted = 0
        for artifact in artifacts:
            try:
                task_was_failed = _fail_expired_artifact_task(artifact.task)
                if not task_was_failed:
                    # A worker may have finalized the metadata and transitioned the
                    # task after the prune query selected its earlier tombstone.
                    # Refreshing the deadline closes that race even when the task
                    # was already terminal and no status transaction was needed.
                    db.session.refresh(artifact)
                    if artifact.expires_at > expires_before:
                        continue
                delete_chart_data_export_artifact_transactionally(artifact)
                deleted += 1
            except Exception:  # pylint: disable=broad-except
                logger.exception(
                    "Failed to prune chart-data export artifact %s",
                    artifact.uuid,
                )

        remaining_rows = None
        if self._max_rows_per_run is not None and self._max_rows_per_run > 0:
            remaining_rows = max(self._max_rows_per_run - len(artifacts), 0)
        if remaining_rows != 0:
            artifact_ttl = timedelta(
                seconds=current_app.config["CHART_DATA_ARTIFACT_TTL_SECONDS"]
            )
            pending_before = datetime.now() - artifact_ttl
            started_before = expires_before - artifact_ttl
            stale_tasks = ChartDataExportArtifactDAO.find_stale_tasks_without_artifacts(
                pending_before,
                started_before,
                remaining_rows,
            )
            for task in stale_tasks:
                try:
                    _fail_expired_artifact_task(task)
                except Exception:  # pylint: disable=broad-except
                    logger.exception(
                        "Failed to recover chart-data export task %s without an "
                        "artifact tombstone",
                        task.uuid,
                    )
        return deleted

    def validate(self) -> None:
        pass
