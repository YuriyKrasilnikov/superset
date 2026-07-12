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
"""Tests for pre-execution chart export planning."""

from unittest.mock import MagicMock

from superset.commands.chart.data.export_planner import (
    ChartDataExportMode,
    ChartDataExportPlanner,
)
from superset.commands.chart.data.streaming_export_command import (
    StreamingExportIneligibility,
)
from superset.common.chart_data import ChartDataResultFormat


def _planner(
    *,
    result_format: ChartDataResultFormat = ChartDataResultFormat.CSV,
    optimize_requested: bool = False,
    async_preferred: bool = False,
    artifact_available: bool = False,
    direct_eligible: bool = False,
) -> tuple[ChartDataExportPlanner, MagicMock, MagicMock]:
    query_context = MagicMock(result_format=result_format)
    command = MagicMock()
    command.prepare.return_value.eligible = direct_eligible
    command.prepare.return_value.ineligibility = (
        None if direct_eligible else StreamingExportIneligibility.RESULT_TRANSFORM
    )
    factory = MagicMock(return_value=command)
    return (
        ChartDataExportPlanner(
            query_context,
            optimize_requested=optimize_requested,
            async_preferred=async_preferred,
            artifact_available=artifact_available,
            direct_command_factory=factory,
        ),
        command,
        factory,
    )


def test_async_preference_uses_available_artifact_without_direct_planning() -> None:
    planner, _, factory = _planner(
        async_preferred=True,
        artifact_available=True,
        optimize_requested=True,
    )

    plan = planner.plan()

    assert plan.mode == ChartDataExportMode.ARTIFACT
    assert plan.preference_applied
    factory.assert_not_called()


def test_optimized_eligible_export_uses_direct_streaming() -> None:
    planner, command, factory = _planner(
        optimize_requested=True,
        direct_eligible=True,
    )

    plan = planner.plan()

    assert plan.mode == ChartDataExportMode.DIRECT
    assert plan.direct_command is command
    factory.assert_called_once_with()


def test_ineligible_direct_export_falls_back_to_artifact() -> None:
    planner, _, _ = _planner(
        optimize_requested=True,
        artifact_available=True,
    )

    plan = planner.plan()

    assert plan.mode == ChartDataExportMode.ARTIFACT
    assert plan.direct_ineligibility == StreamingExportIneligibility.RESULT_TRANSFORM
    assert not plan.preference_applied


def test_unoptimized_or_non_csv_export_remains_materialized() -> None:
    unoptimized, _, unoptimized_factory = _planner(artifact_available=True)
    non_csv, _, non_csv_factory = _planner(
        result_format=ChartDataResultFormat.JSON,
        optimize_requested=True,
        async_preferred=True,
        artifact_available=True,
    )

    assert unoptimized.plan().mode == ChartDataExportMode.MATERIALIZED
    assert non_csv.plan().mode == ChartDataExportMode.MATERIALIZED
    unoptimized_factory.assert_not_called()
    non_csv_factory.assert_not_called()
