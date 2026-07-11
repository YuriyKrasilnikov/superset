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
"""Unit tests for task pruning with external export artifacts."""

from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from superset.commands.tasks.prune import TaskPruneCommand


def test_task_prune_keeps_artifact_task_when_storage_delete_fails(
    mocker: MockerFixture,
) -> None:
    selected = MagicMock()
    selected.scalars.return_value.all.return_value = [1, 2]
    deleted = MagicMock(rowcount=1)
    session = mocker.patch("superset.commands.tasks.prune.db.session")
    session.execute.side_effect = [selected, deleted]
    artifact = MagicMock(
        task_id=1,
        storage_key="b8b61b7b-1cd3-4a31-a74a-0a95341afc06",
    )
    mocker.patch(
        "superset.daos.chart_data_export_artifact."
        "ChartDataExportArtifactDAO.find_by_task_ids",
        return_value=[artifact],
    )
    store = MagicMock()
    store.delete.side_effect = OSError("store unavailable")
    mocker.patch(
        "superset.charts.data.artifacts.get_chart_data_artifact_store",
        return_value=store,
    )

    TaskPruneCommand(retention_period_days=30).run()

    delete_statement = session.execute.call_args_list[1].args[0]
    rendered = str(delete_statement.compile(compile_kwargs={"literal_binds": True}))
    assert "IN (2)" in rendered
    assert "IN (1" not in rendered
    session.commit.assert_called_once_with()
