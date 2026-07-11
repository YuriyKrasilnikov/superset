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
"""Tests for chart-data export artifact lifecycle pruning."""

from datetime import datetime, timedelta
from unittest.mock import MagicMock
from uuid import uuid4

from pytest_mock import MockerFixture
from superset_core.tasks.types import TaskStatus

from superset.commands.chart.data.prune_export_artifacts import (
    ChartDataExportArtifactPruneCommand,
)


def test_prune_export_artifacts_keeps_failed_metadata_addressable(
    mocker: MockerFixture,
) -> None:
    failed = MagicMock(
        uuid="failed",
        expires_at=datetime.min,
        task=MagicMock(status=TaskStatus.FAILURE.value),
    )
    deleted = MagicMock(
        uuid="deleted",
        expires_at=datetime.min,
        task=MagicMock(status=TaskStatus.SUCCESS.value),
    )
    find_expired = mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts."
        "ChartDataExportArtifactDAO.find_expired",
        return_value=[failed, deleted],
    )
    delete_artifact = mocker.patch(
        "superset.charts.data.artifacts.delete_chart_data_export_artifact",
        side_effect=[OSError("store unavailable"), None],
    )
    session = mocker.patch("superset.db.session")

    count = ChartDataExportArtifactPruneCommand(max_rows_per_run=2).run()

    assert count == 1
    find_expired.assert_called_once()
    assert find_expired.call_args.args[1] == 2
    assert delete_artifact.call_args_list[0].args == (failed,)
    assert delete_artifact.call_args_list[1].args == (deleted,)
    session.rollback.assert_called_once_with()
    session.commit.assert_called_once_with()


def test_prune_export_artifacts_fails_active_task_before_deleting(
    mocker: MockerFixture,
) -> None:
    task_uuid = uuid4()
    task = MagicMock(
        uuid=task_uuid,
        status=TaskStatus.IN_PROGRESS.value,
        properties_dict={"execution_mode": "async", "progress_percent": 0.5},
    )
    artifact = MagicMock(uuid="expired-active", task=task)
    mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts."
        "ChartDataExportArtifactDAO.find_expired",
        return_value=[artifact],
    )
    mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts."
        "ChartDataExportArtifactDAO.find_stale_tasks_without_artifacts",
        return_value=[],
    )
    transition = mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts."
        "InternalStatusTransitionCommand"
    )
    transition.return_value.run.return_value = True
    publish_completion = mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts."
        "TaskManager.publish_completion"
    )
    delete_artifact = mocker.patch(
        "superset.charts.data.artifacts.delete_chart_data_export_artifact"
    )
    mocker.patch("superset.db.session")

    count = ChartDataExportArtifactPruneCommand().run()

    assert count == 1
    transition.assert_called_once_with(
        task_uuid=task_uuid,
        new_status=TaskStatus.FAILURE,
        expected_status=[
            TaskStatus.PENDING,
            TaskStatus.IN_PROGRESS,
            TaskStatus.ABORTING,
        ],
        properties={
            "execution_mode": "async",
            "progress_percent": 0.5,
            "error_message": "Chart-data export expired before generation completed",
            "exception_type": "ArtifactGenerationExpired",
        },
        set_ended_at=True,
    )
    publish_completion.assert_called_once_with(
        task_uuid,
        TaskStatus.FAILURE.value,
    )
    delete_artifact.assert_called_once_with(artifact)


def test_prune_export_artifacts_keeps_concurrently_finalized_artifact(
    mocker: MockerFixture,
) -> None:
    task = MagicMock(
        uuid=uuid4(),
        status=TaskStatus.IN_PROGRESS.value,
        properties_dict={"execution_mode": "async"},
    )
    artifact = MagicMock(
        uuid="finalized",
        task=task,
        expires_at=datetime.min,
    )
    mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts."
        "ChartDataExportArtifactDAO.find_expired",
        return_value=[artifact],
    )
    mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts."
        "ChartDataExportArtifactDAO.find_stale_tasks_without_artifacts",
        return_value=[],
    )
    transition = mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts."
        "InternalStatusTransitionCommand"
    )
    transition.return_value.run.return_value = False
    session = mocker.patch("superset.db.session")
    session.refresh.side_effect = lambda item: setattr(
        item,
        "expires_at",
        datetime.now() + timedelta(minutes=5),
    )
    delete_artifact = mocker.patch(
        "superset.charts.data.artifacts.delete_chart_data_export_artifact"
    )

    count = ChartDataExportArtifactPruneCommand().run()

    assert count == 0
    session.refresh.assert_called_once_with(artifact)
    delete_artifact.assert_not_called()


def test_prune_export_artifacts_recovers_stale_task_without_tombstone(
    app_context: None,
    mocker: MockerFixture,
) -> None:
    utc_now = datetime(2026, 7, 10, 12, 0)
    local_now = datetime(2026, 7, 10, 15, 0)
    task = MagicMock(
        uuid=uuid4(),
        status=TaskStatus.IN_PROGRESS.value,
        properties_dict={"execution_mode": "async"},
    )
    mock_datetime = mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts.datetime"
    )
    mock_datetime.now.side_effect = (
        lambda timezone_value=None: utc_now if timezone_value else local_now
    )
    mock_current_app = mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts.current_app"
    )
    mock_current_app.config = {"CHART_DATA_ARTIFACT_TTL_SECONDS": 60 * 60}
    mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts."
        "ChartDataExportArtifactDAO.find_expired",
        return_value=[],
    )
    find_stale = mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts."
        "ChartDataExportArtifactDAO.find_stale_tasks_without_artifacts",
        return_value=[task],
    )
    transition = mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts."
        "InternalStatusTransitionCommand"
    )
    transition.return_value.run.return_value = True
    publish_completion = mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts."
        "TaskManager.publish_completion"
    )

    count = ChartDataExportArtifactPruneCommand().run()

    assert count == 0
    find_stale.assert_called_once()
    pending_before = find_stale.call_args.args[0]
    started_before = find_stale.call_args.args[1]
    assert pending_before == local_now - timedelta(
        seconds=60 * 60,
    )
    assert started_before == utc_now - timedelta(
        seconds=60 * 60,
    )
    assert find_stale.call_args.args[2] is None
    publish_completion.assert_called_once_with(task.uuid, TaskStatus.FAILURE.value)


def test_prune_export_artifacts_recovers_stale_pending_task(
    app_context: None,
    mocker: MockerFixture,
) -> None:
    task = MagicMock(
        uuid=uuid4(),
        status=TaskStatus.PENDING.value,
        properties_dict={"execution_mode": "async"},
    )
    mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts."
        "ChartDataExportArtifactDAO.find_expired",
        return_value=[],
    )
    mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts."
        "ChartDataExportArtifactDAO.find_stale_tasks_without_artifacts",
        return_value=[task],
    )
    transition = mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts."
        "InternalStatusTransitionCommand"
    )
    transition.return_value.run.return_value = True
    publish_completion = mocker.patch(
        "superset.commands.chart.data.prune_export_artifacts."
        "TaskManager.publish_completion"
    )

    ChartDataExportArtifactPruneCommand().run()

    transition.assert_called_once()
    assert TaskStatus.PENDING in transition.call_args.kwargs["expected_status"]
    publish_completion.assert_called_once_with(task.uuid, TaskStatus.FAILURE.value)
