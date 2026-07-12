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
from pathlib import Path
from unittest.mock import MagicMock
from uuid import uuid4

from flask import current_app
from pytest_mock import MockerFixture
from superset_core.tasks.types import TaskScope, TaskStatus

from superset import db
from superset.charts.data.artifacts import (
    delete_chart_data_export_artifact_transactionally,
    FileSystemChartDataArtifactStore,
)
from superset.daos.chart_data_export_artifact import ChartDataExportArtifactDAO
from superset.daos.tasks import TaskDAO
from superset.models.chart_data_export_artifact import (
    ChartDataExportArtifact,
    ChartDataExportArtifactState,
)
from superset.models.tasks import Task
from superset.tasks.api import TaskRestApi
from superset.tasks.chart_data_exports import generate_chart_data_export_artifact
from superset.tasks.executor import execute_task_callable


def test_artifact_task_publishes_downloadable_object_end_to_end(
    app_context: None,
    get_user,
    login_as,
    mocker: MockerFixture,
    tmp_path: Path,
) -> None:
    """Execution fences metadata before exposing one authorized object."""
    login_as("admin")
    admin = get_user("admin")
    store = FileSystemChartDataArtifactStore(tmp_path)
    current_app.config["CHART_DATA_ARTIFACT_STORE"] = store
    current_app.config["CHART_DATA_ARTIFACT_TTL_SECONDS"] = 3600
    task = TaskDAO.create_task(
        task_type="chart_data.generate_export_artifact",
        task_key=f"artifact-e2e-{uuid4()}",
        scope=TaskScope.PRIVATE,
        user_id=admin.id,
    )
    task.created_by = admin
    db.session.commit()
    content = b"city,total\nParis,12\n"
    mocker.patch(
        "superset.tasks.chart_data_exports._materialize_export",
        return_value=(content, "text/csv"),
    )
    mocker.patch(
        "superset.tasks.manager.TaskManager.listen_for_abort",
        return_value=MagicMock(),
    )
    datasource = MagicMock()
    mocker.patch(
        "superset.tasks.api.DatasourceDAO.get_datasource",
        return_value=datasource,
    )
    raise_for_access = mocker.patch(
        "superset.tasks.api.security_manager.raise_for_access"
    )

    artifact: ChartDataExportArtifact | None = None
    try:
        final_task = execute_task_callable(
            task,
            "chart_data.generate_export_artifact",
            generate_chart_data_export_artifact.func,
            (
                {"datasource": {"id": 1, "type": "table"}},
                admin.id,
                "sales.csv",
            ),
            {},
        )

        assert final_task.status == TaskStatus.SUCCESS.value
        artifact = ChartDataExportArtifactDAO.find_by_task_id(task.id)
        assert artifact is not None
        assert artifact.state == ChartDataExportArtifactState.READY.value

        resolved = TaskRestApi._resolve_export_artifact(task.uuid, admin.id)
        assert resolved is not None
        raise_for_access.assert_called_once_with(datasource=datasource)
        with store.open(resolved.storage_key) as artifact_file:
            assert artifact_file.read() == content
    finally:
        if artifact is not None:
            delete_chart_data_export_artifact_transactionally(artifact)
        db.session.delete(task)
        db.session.commit()


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
            task.changed_on = created_on
        tasks.append(task)
        return task

    stale_pending = create_task("pending", TaskStatus.PENDING, old)
    stale_running = create_task("running", TaskStatus.IN_PROGRESS, old)
    stale_aborting = create_task("aborting", TaskStatus.ABORTING, old)
    stale_finalizing = create_task("finalizing", TaskStatus.FINALIZING, old)
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
            cutoff,
        )

        assert {task.id for task in stale} == {
            stale_pending.id,
            stale_running.id,
            stale_aborting.id,
            stale_finalizing.id,
        }
    finally:
        db.session.delete(artifact)
        db.session.flush()
        for task in tasks:
            db.session.delete(task)
        db.session.commit()


def test_artifact_publication_requires_finalizing_task_and_creating_state(
    app_context: None,
    get_user,
    login_as,
) -> None:
    """The artifact and task state form one publication fence."""
    login_as("admin")
    admin = get_user("admin")
    task = TaskDAO.create_task(
        task_type="chart_data.generate_export_artifact",
        task_key=f"artifact-publication-{uuid4()}",
        scope=TaskScope.PRIVATE,
        user_id=admin.id,
    )
    task.created_by = admin
    task.set_status(TaskStatus.IN_PROGRESS)
    db.session.flush()
    artifact = ChartDataExportArtifact(
        task_id=task.id,
        owner_id=admin.id,
        datasource_id="1",
        datasource_type="table",
        storage_key=str(uuid4()),
        filename="pending.csv",
        content_type="application/octet-stream",
        size_bytes=0,
        sha256="",
        expires_at=datetime(2026, 7, 10, 12, 1),
    )
    db.session.add(artifact)
    db.session.commit()

    try:
        expires_at = datetime(2026, 7, 11, 12, 0)
        assert not ChartDataExportArtifactDAO.mark_ready(
            artifact.uuid,
            artifact.storage_key,
            size_bytes=4,
            sha256="a" * 64,
            filename="ready.csv",
            content_type="text/csv",
            expires_at=expires_at,
        )
        db.session.rollback()

        task.set_status(TaskStatus.FINALIZING)
        db.session.commit()
        assert ChartDataExportArtifactDAO.mark_ready(
            artifact.uuid,
            artifact.storage_key,
            size_bytes=4,
            sha256="a" * 64,
            filename="ready.csv",
            content_type="text/csv",
            expires_at=expires_at,
        )
        db.session.commit()
        db.session.refresh(artifact)

        assert artifact.state == ChartDataExportArtifactState.READY.value
        assert artifact.filename == "ready.csv"
        assert not ChartDataExportArtifactDAO.mark_ready(
            artifact.uuid,
            artifact.storage_key,
            size_bytes=4,
            sha256="a" * 64,
            filename="ready.csv",
            content_type="text/csv",
            expires_at=expires_at,
        )
    finally:
        db.session.rollback()
        db.session.delete(artifact)
        db.session.delete(task)
        db.session.commit()
