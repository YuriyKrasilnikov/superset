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
from enum import Enum
from typing import TYPE_CHECKING

from flask import current_app
from superset_core.tasks.types import TaskProperties, TaskStatus

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


class ArtifactRecoveryDisposition(str, Enum):
    """Whether pruning may delete the artifact after task recovery."""

    KEEP = "keep"
    DELETE = "delete"


def _recovery_properties(task: Task) -> TaskProperties:
    return {
        **task.properties_dict,
        "error_message": "Chart-data export expired before generation completed",
        "exception_type": "ArtifactGenerationExpired",
    }


def _publish_completion(task: Task, status: TaskStatus) -> None:
    try:
        TaskManager.publish_completion(task.uuid, status.value)
    except Exception:  # pylint: disable=broad-except
        logger.exception(
            "Failed to publish expiration outcome for chart-data export task %s",
            task.uuid,
        )


def _recover_expired_artifact_task(
    task: Task | None,
    recovery_before: datetime,
) -> ArtifactRecoveryDisposition:
    """Advance one expired task through the authoritative recovery graph."""
    if task is None or task.status not in ACTIVE_STATES:
        return ArtifactRecoveryDisposition.DELETE

    status = TaskStatus(task.status)
    if status == TaskStatus.IN_PROGRESS:
        transitioned = InternalStatusTransitionCommand(
            task_uuid=task.uuid,
            new_status=TaskStatus.ABORTING,
            expected_status=TaskStatus.IN_PROGRESS,
        ).run()
        if transitioned:
            TaskManager.publish_abort(task.uuid)
        return ArtifactRecoveryDisposition.KEEP

    if status in (TaskStatus.FINALIZING, TaskStatus.ABORTING):
        changed_on = task.changed_on
        if changed_on is None or changed_on > recovery_before:
            return ArtifactRecoveryDisposition.KEEP
        terminal_status = (
            TaskStatus.FAILURE
            if status == TaskStatus.FINALIZING
            else TaskStatus.TIMED_OUT
        )
        transitioned = InternalStatusTransitionCommand(
            task_uuid=task.uuid,
            new_status=terminal_status,
            expected_status=status,
            properties=_recovery_properties(task),
            set_ended_at=True,
        ).run()
        if transitioned:
            _publish_completion(task, terminal_status)
            return ArtifactRecoveryDisposition.DELETE
        return ArtifactRecoveryDisposition.KEEP

    transitioned = InternalStatusTransitionCommand(
        task_uuid=task.uuid,
        new_status=TaskStatus.FAILURE,
        expected_status=TaskStatus.PENDING,
        properties=_recovery_properties(task),
        set_ended_at=True,
    ).run()
    if transitioned:
        _publish_completion(task, TaskStatus.FAILURE)
        return ArtifactRecoveryDisposition.DELETE
    return ArtifactRecoveryDisposition.KEEP


class ChartDataExportArtifactPruneCommand(BaseCommand):
    """Remove expired export objects without orphaning their storage keys."""

    def __init__(self, max_rows_per_run: int | None = None) -> None:
        self._max_rows_per_run = max_rows_per_run

    def run(self) -> int:
        """Delete expired artifacts independently and return the deleted count."""
        expires_before = datetime.now(timezone.utc).replace(tzinfo=None)
        recovery_grace = timedelta(
            seconds=current_app.config["CHART_DATA_ARTIFACT_RECOVERY_GRACE_SECONDS"]
        )
        recovery_before = expires_before - recovery_grace
        artifacts = ChartDataExportArtifactDAO.find_expired(
            expires_before,
            self._max_rows_per_run,
        )
        deleted = 0
        for artifact in artifacts:
            try:
                disposition = _recover_expired_artifact_task(
                    artifact.task,
                    recovery_before,
                )
                if disposition == ArtifactRecoveryDisposition.KEEP:
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
            pending_before = expires_before - artifact_ttl
            started_before = expires_before - artifact_ttl
            stale_tasks = ChartDataExportArtifactDAO.find_stale_tasks_without_artifacts(
                pending_before,
                started_before,
                recovery_before,
                remaining_rows,
            )
            for task in stale_tasks:
                try:
                    _recover_expired_artifact_task(task, recovery_before)
                except Exception:  # pylint: disable=broad-except
                    logger.exception(
                        "Failed to recover chart-data export task %s without an "
                        "artifact tombstone",
                        task.uuid,
                    )
        return deleted

    def validate(self) -> None:
        pass
