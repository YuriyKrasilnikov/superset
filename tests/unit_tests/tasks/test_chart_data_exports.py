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
"""Tests for chart-data artifact task boundaries."""

from datetime import datetime, timezone
from unittest.mock import call, MagicMock

import pytest
from pytest_mock import MockerFixture
from superset_core.tasks.types import TaskStatus

from superset.common.chart_data import ChartDataResultFormat, ChartDataResultType
from superset.tasks.chart_data_exports import (
    generate_chart_data_export_artifact,
    serialize_query_context_for_artifact,
)


def test_serialize_query_context_for_artifact_is_json_safe() -> None:
    timestamp = datetime(2026, 7, 10, 12, 0, tzinfo=timezone.utc)
    query_context = MagicMock()
    query_context.cache_values = {
        "datasource": {"id": 7, "type": "table"},
        "queries": [{"from_dttm": timestamp}],
    }
    query_context.custom_cache_timeout = 30
    query_context.force = True
    query_context.form_data = {"slice_id": 9}
    query_context.result_format = ChartDataResultFormat.CSV
    query_context.result_type = ChartDataResultType.FULL

    payload = serialize_query_context_for_artifact(query_context)

    assert payload["queries"][0]["from_dttm"] == timestamp.isoformat()
    assert payload["result_format"] == "csv"
    assert payload["result_type"] == "full"
    assert payload["custom_cache_timeout"] == 30
    assert payload["force"] is True


def test_artifact_task_rejects_mismatched_task_owner(
    mocker: MockerFixture,
) -> None:
    context = MagicMock(task_uuid="b8b61b7b-1cd3-4a31-a74a-0a95341afc06")
    task_record = MagicMock(user_id=8)
    mocker.patch("superset.tasks.chart_data_exports.get_context", return_value=context)
    mocker.patch(
        "superset.tasks.chart_data_exports.TaskDAO.find_one_or_none",
        return_value=task_record,
    )
    mocker.patch(
        "superset.tasks.chart_data_exports.security_manager.get_user_by_id",
        return_value=MagicMock(),
    )
    materialize = mocker.patch("superset.tasks.chart_data_exports._materialize_export")

    with pytest.raises(RuntimeError, match="ownership could not be verified"):
        generate_chart_data_export_artifact.func({}, 7, "export.csv")

    materialize.assert_not_called()


def test_artifact_task_forces_initiating_user_context(
    app_context: None,
    mocker: MockerFixture,
) -> None:
    context = MagicMock(task_uuid="b8b61b7b-1cd3-4a31-a74a-0a95341afc06")
    task_record = MagicMock(id=19, user_id=7)
    user = MagicMock(id=7)
    store = MagicMock()
    store.write.side_effect = lambda _key, chunks: sum(len(chunk) for chunk in chunks)
    mocker.patch("superset.tasks.chart_data_exports.get_context", return_value=context)
    mocker.patch(
        "superset.tasks.chart_data_exports.TaskDAO.find_one_or_none",
        return_value=task_record,
    )
    mocker.patch(
        "superset.tasks.chart_data_exports.security_manager.get_user_by_id",
        return_value=user,
    )
    mocker.patch(
        "superset.tasks.chart_data_exports.get_chart_data_artifact_store",
        return_value=store,
    )
    override_user = mocker.patch("superset.tasks.chart_data_exports.override_user")
    mocker.patch(
        "superset.tasks.chart_data_exports._materialize_export",
        return_value=(b"a,b\n1,2\n", "text/csv"),
    )
    mocker.patch("superset.tasks.chart_data_exports.ChartDataExportArtifactDAO.create")
    mocker.patch("superset.db.session")

    generate_chart_data_export_artifact.func(
        {"datasource": {"id": 7, "type": "table"}},
        7,
        "export.csv",
    )

    assert override_user.call_args_list == [call(user), call(user)]


def test_artifact_task_persists_cleanup_key_before_storage_write(
    app_context: None,
    mocker: MockerFixture,
) -> None:
    context = MagicMock(task_uuid="b8b61b7b-1cd3-4a31-a74a-0a95341afc06")
    task_record = MagicMock(id=19, user_id=7)
    events: list[str] = []
    store = MagicMock()

    def fail_write(_key: str, _chunks: object) -> int:
        events.append("write")
        raise OSError("store failed")

    store.write.side_effect = fail_write
    mocker.patch("superset.tasks.chart_data_exports.get_context", return_value=context)
    mocker.patch(
        "superset.tasks.chart_data_exports.TaskDAO.find_one_or_none",
        return_value=task_record,
    )
    mocker.patch(
        "superset.tasks.chart_data_exports.security_manager.get_user_by_id",
        return_value=MagicMock(id=7),
    )
    mocker.patch(
        "superset.tasks.chart_data_exports.get_chart_data_artifact_store",
        return_value=store,
    )
    mocker.patch("superset.tasks.chart_data_exports.override_user")
    mocker.patch(
        "superset.tasks.chart_data_exports._materialize_export",
        return_value=(b"a,b\n1,2\n", "text/csv"),
    )
    create = mocker.patch(
        "superset.tasks.chart_data_exports.ChartDataExportArtifactDAO.create",
        side_effect=lambda **_kwargs: events.append("metadata"),
    )
    mocker.patch("superset.db.session")

    with pytest.raises(OSError, match="store failed"):
        generate_chart_data_export_artifact.func(
            {"datasource": {"id": 7, "type": "table"}},
            7,
            "export.csv",
        )

    assert events == ["metadata", "write"]
    create.assert_called_once()


def test_artifact_task_persists_tombstone_before_materialization(
    app_context: None,
    mocker: MockerFixture,
) -> None:
    context = MagicMock(task_uuid="b8b61b7b-1cd3-4a31-a74a-0a95341afc06")
    task_record = MagicMock(id=19, user_id=7)
    events: list[str] = []
    store = MagicMock()
    mocker.patch("superset.tasks.chart_data_exports.get_context", return_value=context)
    mocker.patch(
        "superset.tasks.chart_data_exports.TaskDAO.find_one_or_none",
        return_value=task_record,
    )
    mocker.patch(
        "superset.tasks.chart_data_exports.security_manager.get_user_by_id",
        return_value=MagicMock(id=7),
    )
    mocker.patch(
        "superset.tasks.chart_data_exports.get_chart_data_artifact_store",
        return_value=store,
    )
    mocker.patch("superset.tasks.chart_data_exports.override_user")

    def fail_materialize(_payload: dict[str, object]) -> tuple[bytes, str]:
        events.append("materialize")
        raise ValueError("query failed")

    materialize = mocker.patch(
        "superset.tasks.chart_data_exports._materialize_export",
        side_effect=fail_materialize,
    )
    create = mocker.patch(
        "superset.tasks.chart_data_exports.ChartDataExportArtifactDAO.create",
        side_effect=lambda **_kwargs: events.append("metadata"),
    )
    mocker.patch("superset.db.session")

    with pytest.raises(ValueError, match="query failed"):
        generate_chart_data_export_artifact.func(
            {"datasource": {"id": 7, "type": "table"}},
            7,
            "export.csv",
        )

    assert events == ["metadata", "materialize"]
    artifact = create.call_args.kwargs["item"]
    assert artifact.content_type == "application/octet-stream"
    assert artifact.size_bytes == 0
    assert artifact.sha256 == ""
    materialize.assert_called_once()
    store.write.assert_not_called()


def test_artifact_task_refreshes_ttl_after_durable_write(
    app_context: None,
    mocker: MockerFixture,
) -> None:
    context = MagicMock(task_uuid="b8b61b7b-1cd3-4a31-a74a-0a95341afc06")
    task_record = MagicMock(id=19, user_id=7)
    store = MagicMock()
    store.write.side_effect = lambda _key, chunks: sum(len(chunk) for chunk in chunks)
    initial_expiration = datetime(2026, 7, 10, 12, 0)
    final_expiration = datetime(2026, 7, 10, 13, 0)
    mocker.patch("superset.tasks.chart_data_exports.get_context", return_value=context)
    mocker.patch(
        "superset.tasks.chart_data_exports.TaskDAO.find_one_or_none",
        return_value=task_record,
    )
    mocker.patch(
        "superset.tasks.chart_data_exports.security_manager.get_user_by_id",
        return_value=MagicMock(id=7),
    )
    mocker.patch(
        "superset.tasks.chart_data_exports.get_chart_data_artifact_store",
        return_value=store,
    )
    mocker.patch("superset.tasks.chart_data_exports.override_user")
    mocker.patch(
        "superset.tasks.chart_data_exports._materialize_export",
        return_value=(b"a,b\n1,2\n", "text/csv"),
    )
    expiration = mocker.patch(
        "superset.tasks.chart_data_exports._artifact_expiration",
        side_effect=[initial_expiration, final_expiration],
    )
    create = mocker.patch(
        "superset.tasks.chart_data_exports.ChartDataExportArtifactDAO.create"
    )
    mocker.patch("superset.db.session")

    generate_chart_data_export_artifact.func(
        {"datasource": {"id": 7, "type": "table"}},
        7,
        "export.csv",
    )

    artifact = create.call_args.kwargs["item"]
    assert artifact.expires_at == final_expiration
    assert artifact.content_type == "text/csv"
    assert artifact.size_bytes == len(b"a,b\n1,2\n")
    assert context.update_task.call_args.kwargs["payload"]["expires_at"] == (
        final_expiration.isoformat()
    )
    assert expiration.call_count == 2


def test_artifact_task_cleanup_keeps_successful_artifact(
    app_context: None,
    mocker: MockerFixture,
) -> None:
    context = MagicMock(task_uuid="b8b61b7b-1cd3-4a31-a74a-0a95341afc06")
    task_record = MagicMock(id=19, user_id=7)
    store = MagicMock()
    store.write.side_effect = lambda _key, chunks: sum(len(chunk) for chunk in chunks)
    mocker.patch("superset.tasks.chart_data_exports.get_context", return_value=context)
    mocker.patch(
        "superset.tasks.chart_data_exports.TaskDAO.find_one_or_none",
        return_value=task_record,
    )
    mocker.patch(
        "superset.tasks.chart_data_exports.TaskDAO.get_status",
        return_value=TaskStatus.SUCCESS.value,
    )
    mocker.patch(
        "superset.tasks.chart_data_exports.security_manager.get_user_by_id",
        return_value=MagicMock(id=7),
    )
    mocker.patch(
        "superset.tasks.chart_data_exports.get_chart_data_artifact_store",
        return_value=store,
    )
    mocker.patch("superset.tasks.chart_data_exports.override_user")
    mocker.patch(
        "superset.tasks.chart_data_exports._materialize_export",
        return_value=(b"a,b\n1,2\n", "text/csv"),
    )
    mocker.patch("superset.tasks.chart_data_exports.ChartDataExportArtifactDAO.create")
    find_artifact = mocker.patch(
        "superset.tasks.chart_data_exports.ChartDataExportArtifactDAO.find_by_task_id"
    )
    mocker.patch("superset.db.session")

    generate_chart_data_export_artifact.func(
        {"datasource": {"id": 7, "type": "table"}},
        7,
        "export.csv",
    )
    cleanup = context.on_cleanup.call_args.args[0]
    cleanup()

    find_artifact.assert_not_called()
    store.delete.assert_not_called()


def test_artifact_task_bounds_persisted_filename(
    app_context: None,
    mocker: MockerFixture,
) -> None:
    context = MagicMock(task_uuid="b8b61b7b-1cd3-4a31-a74a-0a95341afc06")
    task_record = MagicMock(id=19, user_id=7)
    store = MagicMock()
    store.write.side_effect = lambda _key, chunks: sum(len(chunk) for chunk in chunks)
    mocker.patch("superset.tasks.chart_data_exports.get_context", return_value=context)
    mocker.patch(
        "superset.tasks.chart_data_exports.TaskDAO.find_one_or_none",
        return_value=task_record,
    )
    mocker.patch(
        "superset.tasks.chart_data_exports.security_manager.get_user_by_id",
        return_value=MagicMock(id=7),
    )
    mocker.patch(
        "superset.tasks.chart_data_exports.get_chart_data_artifact_store",
        return_value=store,
    )
    mocker.patch("superset.tasks.chart_data_exports.override_user")
    mocker.patch(
        "superset.tasks.chart_data_exports._materialize_export",
        return_value=(b"a,b\n1,2\n", "text/csv"),
    )
    create = mocker.patch(
        "superset.tasks.chart_data_exports.ChartDataExportArtifactDAO.create"
    )
    mocker.patch("superset.db.session")

    generate_chart_data_export_artifact.func(
        {"datasource": {"id": 7, "type": "table"}},
        7,
        f"{'a' * 400}.csv",
    )

    artifact = create.call_args.kwargs["item"]
    assert len(artifact.filename) == 255
    assert artifact.filename.endswith(".csv")
