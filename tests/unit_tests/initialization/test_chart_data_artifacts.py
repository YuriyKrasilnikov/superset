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
"""Startup invariants for asynchronous chart-data artifacts."""

from pathlib import Path

import pytest
from pytest_mock import MockerFixture

from superset.app import SupersetApp
from superset.charts.data.artifacts import FileSystemChartDataArtifactStore
from superset.initialization import SupersetAppInitializer


def _initializer(app: SupersetApp) -> SupersetAppInitializer:
    initializer = SupersetAppInitializer.__new__(SupersetAppInitializer)
    initializer.superset_app = app
    initializer.config = app.config
    return initializer


def test_artifact_exports_require_global_task_framework(
    mocker: MockerFixture,
) -> None:
    app = SupersetApp(__name__)
    enabled = mocker.patch(
        "superset.initialization.feature_flag_manager.is_feature_enabled",
        side_effect=lambda flag: flag == "CHART_DATA_ASYNC_EXPORTS",
    )

    with (
        app.app_context(),
        pytest.raises(
            RuntimeError,
            match="requires GLOBAL_TASK_FRAMEWORK",
        ),
    ):
        _initializer(app).configure_task_manager()

    assert enabled.call_count == 2


def test_artifact_exports_require_valid_store(mocker: MockerFixture) -> None:
    app = SupersetApp(__name__)
    app.config["CHART_DATA_ARTIFACT_STORE"] = object()
    mocker.patch(
        "superset.initialization.feature_flag_manager.is_feature_enabled",
        return_value=True,
    )

    with (
        app.app_context(),
        pytest.raises(
            RuntimeError,
            match="must implement ChartDataArtifactStore",
        ),
    ):
        _initializer(app).configure_task_manager()


@pytest.mark.parametrize("ttl", [0, -1, True, "3600"])
def test_artifact_exports_require_positive_integer_ttl(
    ttl: object,
    tmp_path: Path,
    mocker: MockerFixture,
) -> None:
    app = SupersetApp(__name__)
    app.config["CHART_DATA_ARTIFACT_STORE"] = FileSystemChartDataArtifactStore(tmp_path)
    app.config["CHART_DATA_ARTIFACT_TTL_SECONDS"] = ttl
    mocker.patch(
        "superset.initialization.feature_flag_manager.is_feature_enabled",
        return_value=True,
    )

    with (
        app.app_context(),
        pytest.raises(
            RuntimeError,
            match="must be a positive integer",
        ),
    ):
        _initializer(app).configure_task_manager()


def test_valid_artifact_configuration_initializes_task_manager(
    tmp_path: Path,
    mocker: MockerFixture,
) -> None:
    app = SupersetApp(__name__)
    app.config["CHART_DATA_ARTIFACT_STORE"] = FileSystemChartDataArtifactStore(tmp_path)
    app.config["CHART_DATA_ARTIFACT_TTL_SECONDS"] = 3600
    mocker.patch(
        "superset.initialization.feature_flag_manager.is_feature_enabled",
        return_value=True,
    )
    init_app = mocker.patch("superset.tasks.manager.TaskManager.init_app")

    with app.app_context():
        _initializer(app).configure_task_manager()

    init_app.assert_called_once_with(app)
