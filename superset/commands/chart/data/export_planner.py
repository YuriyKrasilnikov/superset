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
"""Pre-execution planning for chart-data CSV export transports."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from superset.commands.chart.data.streaming_export_command import (
    StreamingCSVExportCommand,
    StreamingExportIneligibility,
)
from superset.common.chart_data import ChartDataResultFormat

if TYPE_CHECKING:
    from superset.common.query_context import QueryContext


class ChartDataExportMode(str, Enum):
    """Execution and delivery strategies for one chart export."""

    MATERIALIZED = "materialized"
    DIRECT = "direct"


@dataclass(frozen=True)
class ChartDataExportPlan:
    """A selected strategy plus diagnostics from direct planning."""

    mode: ChartDataExportMode
    direct_command: StreamingCSVExportCommand | None = None
    direct_ineligibility: StreamingExportIneligibility | None = None


class ChartDataExportPlanner:
    """Select one export strategy without executing the source query."""

    def __init__(
        self,
        query_context: QueryContext,
        *,
        optimize_requested: bool,
        direct_command_factory: Callable[[], StreamingCSVExportCommand],
    ) -> None:
        self._query_context = query_context
        self._optimize_requested = optimize_requested
        self._direct_command_factory = direct_command_factory

    def plan(self) -> ChartDataExportPlan:
        """Return a deterministic plan from request intent and capabilities."""
        if (
            self._query_context.result_format != ChartDataResultFormat.CSV
            or not self._optimize_requested
        ):
            return ChartDataExportPlan(ChartDataExportMode.MATERIALIZED)

        direct_command = self._direct_command_factory()
        preparation = direct_command.prepare()
        if preparation.eligible:
            return ChartDataExportPlan(
                ChartDataExportMode.DIRECT,
                direct_command=direct_command,
            )
        return ChartDataExportPlan(
            ChartDataExportMode.MATERIALIZED,
            direct_ineligibility=preparation.ineligibility,
        )
