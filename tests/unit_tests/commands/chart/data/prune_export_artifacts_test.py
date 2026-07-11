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

from unittest.mock import MagicMock

from pytest_mock import MockerFixture

from superset.commands.chart.data.prune_export_artifacts import (
    ChartDataExportArtifactPruneCommand,
)


def test_prune_export_artifacts_keeps_failed_metadata_addressable(
    mocker: MockerFixture,
) -> None:
    failed = MagicMock(uuid="failed")
    deleted = MagicMock(uuid="deleted")
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
