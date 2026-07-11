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
"""Integration tests for chart-data export artifact recovery queries."""

from datetime import datetime, timedelta
from uuid import uuid4

from superset_core.tasks.types import TaskScope, TaskStatus

from superset import db
from superset.daos.chart_data_export_artifact import ChartDataExportArtifactDAO
from superset.daos.tasks import TaskDAO
from superset.models.chart_data_export_artifact import ChartDataExportArtifact
from superset.models.tasks import Task


def test_stale_export_tasks_without_artifacts_uses_state_specific_clock(
    app_context: None,
    get_user,
    login_as,
) -> None:
    """Pending queue time and running execution time share one recovery deadline."""
    login_as("admin")
    admin = get_user("admin")
    cutoff = datetime(2026, 7, 10, 12, 0)
    old = cutoff - timedelta(minutes=1)
    recent = cutoff + timedelta(minutes=1)
    tasks: list[Task] = []

    def create_task(
        suffix: str,
        status: TaskStatus,
        created_on: datetime,
        *,
        task_type: str = "chart_data.generate_export_artifact",
    ) -> Task:
        task = TaskDAO.create_task(
            task_type=task_type,
            task_key=f"artifact-recovery-{suffix}-{uuid4()}",
            scope=TaskScope.PRIVATE,
            user_id=admin.id,
        )
        task.created_by = admin
        task.created_on = created_on
        if status != TaskStatus.PENDING:
            task.set_status(TaskStatus.IN_PROGRESS)
            task.started_at = created_on
            if status != TaskStatus.IN_PROGRESS:
                task.set_status(status)
        tasks.append(task)
        return task

    stale_pending = create_task("pending", TaskStatus.PENDING, old)
    stale_running = create_task("running", TaskStatus.IN_PROGRESS, old)
    stale_aborting = create_task("aborting", TaskStatus.ABORTING, old)
    create_task("recent", TaskStatus.PENDING, recent)
    create_task("other-type", TaskStatus.PENDING, old, task_type="other.task")
    create_task("terminal", TaskStatus.FAILURE, old)
    covered = create_task("covered", TaskStatus.PENDING, old)
    db.session.flush()
    artifact = ChartDataExportArtifact(
        task_id=covered.id,
        owner_id=admin.id,
        datasource_id="1",
        datasource_type="table",
        storage_key=str(uuid4()),
        filename="pending.csv",
        content_type="application/octet-stream",
        size_bytes=0,
        sha256="",
        expires_at=recent,
    )
    db.session.add(artifact)
    db.session.commit()

    try:
        stale = ChartDataExportArtifactDAO.find_stale_tasks_without_artifacts(
            cutoff,
            cutoff,
        )

        assert {task.id for task in stale} == {
            stale_pending.id,
            stale_running.id,
            stale_aborting.id,
        }
    finally:
        db.session.delete(artifact)
        db.session.flush()
        for task in tasks:
            db.session.delete(task)
        db.session.commit()
