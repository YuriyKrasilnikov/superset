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
"""Authorization tests for private chart-data export artifacts."""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from uuid import uuid4

from pytest_mock import MockerFixture
from superset_core.tasks.types import TaskStatus

from superset.tasks.api import TaskRestApi


def _successful_task(owner_id: int) -> MagicMock:
    task = MagicMock()
    task.id = 19
    task.user_id = owner_id
    task.status = TaskStatus.SUCCESS.value
    return task


def _artifact(owner_id: int, expired: bool = False) -> MagicMock:
    artifact = MagicMock()
    artifact.owner_id = owner_id
    artifact.datasource_type = "table"
    artifact.datasource_id = "7"
    offset = timedelta(seconds=-1 if expired else 60)
    artifact.expires_at = datetime.now(timezone.utc) + offset
    return artifact


def test_resolve_export_artifact_rejects_non_owner(
    mocker: MockerFixture,
) -> None:
    task = _successful_task(owner_id=7)
    mocker.patch(
        "superset.daos.tasks.TaskDAO.find_one_or_none",
        return_value=task,
    )
    mocker.patch(
        "superset.tasks.api.security_manager.is_admin",
        return_value=False,
    )
    find_artifact = mocker.patch(
        "superset.tasks.api.ChartDataExportArtifactDAO.find_by_task_id"
    )

    assert TaskRestApi._resolve_export_artifact(uuid4(), user_id=8) is None
    find_artifact.assert_not_called()


def test_resolve_export_artifact_reauthorizes_datasource(
    mocker: MockerFixture,
) -> None:
    task = _successful_task(owner_id=7)
    artifact = _artifact(owner_id=7)
    datasource = MagicMock()
    mocker.patch(
        "superset.daos.tasks.TaskDAO.find_one_or_none",
        return_value=task,
    )
    mocker.patch(
        "superset.tasks.api.ChartDataExportArtifactDAO.find_by_task_id",
        return_value=artifact,
    )
    get_datasource = mocker.patch(
        "superset.tasks.api.DatasourceDAO.get_datasource",
        return_value=datasource,
    )
    security_manager = mocker.patch("superset.tasks.api.security_manager")

    assert TaskRestApi._resolve_export_artifact(uuid4(), user_id=7) is artifact
    get_datasource.assert_called_once()
    security_manager.raise_for_access.assert_called_once_with(datasource=datasource)


def test_resolve_export_artifact_deletes_expired_object_before_metadata(
    mocker: MockerFixture,
) -> None:
    task = _successful_task(owner_id=7)
    artifact = _artifact(owner_id=7, expired=True)
    mocker.patch(
        "superset.daos.tasks.TaskDAO.find_one_or_none",
        return_value=task,
    )
    mocker.patch(
        "superset.tasks.api.ChartDataExportArtifactDAO.find_by_task_id",
        return_value=artifact,
    )
    delete_artifact = mocker.patch(
        "superset.charts.data.artifacts.delete_chart_data_export_artifact"
    )
    session = mocker.patch("superset.db.session")
    get_datasource = mocker.patch("superset.tasks.api.DatasourceDAO.get_datasource")

    assert TaskRestApi._resolve_export_artifact(uuid4(), user_id=7) is None
    delete_artifact.assert_called_once_with(artifact)
    session.commit.assert_called_once_with()
    get_datasource.assert_not_called()
