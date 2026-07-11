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
"""Tests for chart-data artifact storage contracts."""

import stat
from pathlib import Path

import pytest
from flask import Flask

from superset.charts.data.artifacts import (
    FileSystemChartDataArtifactStore,
    get_chart_data_artifact_store,
    validate_artifact_key,
)

ARTIFACT_KEY = "b8b61b7b-1cd3-4a31-a74a-0a95341afc06"


@pytest.mark.parametrize(
    "key",
    [
        "../artifact",
        "/absolute/artifact",
        "B8B61B7B-1CD3-4A31-A74A-0A95341AFC06",
        "{b8b61b7b-1cd3-4a31-a74a-0a95341afc06}",
        "not-a-uuid",
    ],
)
def test_validate_artifact_key_rejects_noncanonical_keys(key: str) -> None:
    with pytest.raises(ValueError, match="UUID|uuid|canonical"):
        validate_artifact_key(key)


def test_filesystem_store_writes_reads_and_deletes_atomically(
    tmp_path: Path,
) -> None:
    store = FileSystemChartDataArtifactStore(tmp_path)

    assert store.write(ARTIFACT_KEY, [b"first", b"-second"]) == 12
    with store.open(ARTIFACT_KEY) as artifact:
        assert artifact.read() == b"first-second"
    assert stat.S_IMODE((tmp_path / ARTIFACT_KEY).stat().st_mode) == 0o600
    assert not (tmp_path / f"{ARTIFACT_KEY}.partial").exists()

    (tmp_path / f"{ARTIFACT_KEY}.partial").write_bytes(b"stale")
    store.delete(ARTIFACT_KEY)
    store.delete(ARTIFACT_KEY)
    assert not (tmp_path / ARTIFACT_KEY).exists()
    assert not (tmp_path / f"{ARTIFACT_KEY}.partial").exists()


def test_filesystem_store_removes_partial_file_when_chunks_fail(
    tmp_path: Path,
) -> None:
    store = FileSystemChartDataArtifactStore(tmp_path)

    def failing_chunks():
        yield b"partial"
        raise RuntimeError("source failed")

    with pytest.raises(RuntimeError, match="source failed"):
        store.write(ARTIFACT_KEY, failing_chunks())

    assert list(tmp_path.iterdir()) == []


def test_get_artifact_store_fails_closed_for_invalid_config() -> None:
    app = Flask(__name__)
    app.config["CHART_DATA_ARTIFACT_STORE"] = object()

    with app.app_context(), pytest.raises(RuntimeError):
        get_chart_data_artifact_store()


def test_get_artifact_store_returns_protocol_implementation(tmp_path: Path) -> None:
    app = Flask(__name__)
    store = FileSystemChartDataArtifactStore(tmp_path)
    app.config["CHART_DATA_ARTIFACT_STORE"] = store

    with app.app_context():
        assert get_chart_data_artifact_store() is store
