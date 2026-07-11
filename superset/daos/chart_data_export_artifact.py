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
"""DAO for chart-data export artifact metadata."""

from __future__ import annotations

from datetime import datetime
from typing import Sequence, TYPE_CHECKING
from uuid import UUID

from sqlalchemy import and_, or_
from superset_core.tasks.types import TaskStatus

from superset.daos.base import BaseDAO
from superset.models.chart_data_export_artifact import ChartDataExportArtifact

if TYPE_CHECKING:
    from superset.models.tasks import Task


class ChartDataExportArtifactDAO(BaseDAO[ChartDataExportArtifact]):
    """Internal metadata access; HTTP authorization is anchored to TaskDAO."""

    @classmethod
    def find_by_task_id(cls, task_id: int) -> ChartDataExportArtifact | None:
        return cls.find_one_or_none(task_id=task_id, skip_base_filter=True)

    @classmethod
    def find_by_uuid(cls, artifact_uuid: UUID) -> ChartDataExportArtifact | None:
        return cls.find_one_or_none(uuid=artifact_uuid, skip_base_filter=True)

    @classmethod
    def find_by_task_ids(cls, task_ids: Sequence[int]) -> list[ChartDataExportArtifact]:
        return cls.find_by_ids(
            task_ids,
            id_column="task_id",
            skip_base_filter=True,
        )

    @classmethod
    def find_expired(
        cls,
        expires_before: datetime,
        max_rows: int | None = None,
    ) -> list[ChartDataExportArtifact]:
        from superset import db

        query = (
            db.session.query(ChartDataExportArtifact)
            .filter(ChartDataExportArtifact.expires_at <= expires_before)
            .order_by(
                ChartDataExportArtifact.expires_at.asc(),
                ChartDataExportArtifact.id.asc(),
            )
        )
        if max_rows is not None and max_rows > 0:
            query = query.limit(max_rows)
        return query.all()

    @classmethod
    def find_stale_tasks_without_artifacts(
        cls,
        pending_before: datetime,
        started_before: datetime,
        max_rows: int | None = None,
    ) -> list[Task]:
        """Find overdue export tasks that never persisted a cleanup tombstone."""
        from superset import db
        from superset.models.tasks import Task

        query = (
            db.session.query(Task)
            .outerjoin(
                ChartDataExportArtifact,
                ChartDataExportArtifact.task_id == Task.id,
            )
            .filter(
                Task.task_type == "chart_data.generate_export_artifact",
                or_(
                    and_(
                        Task.status == TaskStatus.PENDING.value,
                        Task.created_on <= pending_before,
                    ),
                    and_(
                        Task.status.in_(
                            [
                                TaskStatus.IN_PROGRESS.value,
                                TaskStatus.ABORTING.value,
                            ]
                        ),
                        Task.started_at <= started_before,
                    ),
                ),
                ChartDataExportArtifact.id.is_(None),
            )
            .order_by(Task.created_on.asc(), Task.id.asc())
        )
        if max_rows is not None and max_rows > 0:
            query = query.limit(max_rows)
        return query.all()
