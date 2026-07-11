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
"""Lifecycle command for expired chart-data export artifacts."""

import logging
from datetime import datetime, timezone

from superset.charts.data.artifacts import (
    delete_chart_data_export_artifact_transactionally,
)
from superset.commands.base import BaseCommand
from superset.daos.chart_data_export_artifact import ChartDataExportArtifactDAO

logger = logging.getLogger(__name__)


class ChartDataExportArtifactPruneCommand(BaseCommand):
    """Remove expired export objects without orphaning their storage keys."""

    def __init__(self, max_rows_per_run: int | None = None) -> None:
        self._max_rows_per_run = max_rows_per_run

    def run(self) -> int:
        """Delete expired artifacts independently and return the deleted count."""
        expires_before = datetime.now(timezone.utc).replace(tzinfo=None)
        artifacts = ChartDataExportArtifactDAO.find_expired(
            expires_before,
            self._max_rows_per_run,
        )
        deleted = 0
        for artifact in artifacts:
            try:
                delete_chart_data_export_artifact_transactionally(artifact)
                deleted += 1
            except Exception:  # pylint: disable=broad-except
                logger.exception(
                    "Failed to prune chart-data export artifact %s",
                    artifact.uuid,
                )
        return deleted

    def validate(self) -> None:
        pass
