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
"""GTF task that materializes non-direct chart CSV exports into artifacts."""

from __future__ import annotations

import hashlib
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable
from uuid import uuid4

from flask import current_app, g
from superset_core.tasks.types import TaskScope, TaskStatus
from werkzeug.utils import secure_filename

from superset import security_manager
from superset.charts.client_processing import apply_client_processing
from superset.charts.data.artifacts import (
    delete_chart_data_export_artifact_transactionally,
    get_chart_data_artifact_store,
)
from superset.charts.schemas import ChartDataQueryContextSchema
from superset.commands.chart.data.get_data_command import ChartDataCommand
from superset.common.chart_data import ChartDataResultFormat, ChartDataResultType
from superset.daos.chart_data_export_artifact import ChartDataExportArtifactDAO
from superset.daos.tasks import TaskDAO
from superset.models.chart_data_export_artifact import ChartDataExportArtifact
from superset.tasks.ambient_context import get_context
from superset.tasks.decorators import task
from superset.utils import json
from superset.utils.core import create_zip, override_user
from superset.utils.decorators import transaction

_WRITE_CHUNK_SIZE = 1024 * 1024
_MAX_ARTIFACT_FILENAME_LENGTH = 255
_INCOMPLETE_ARTIFACT_CONTENT_TYPE = "application/octet-stream"


def _query_context_payload(query_context: Any) -> dict[str, Any]:
    """Serialize the stable request fields needed to reconstruct a context."""
    return {
        **query_context.cache_values,
        "custom_cache_timeout": query_context.custom_cache_timeout,
        "force": query_context.force,
        "form_data": query_context.form_data,
        "result_format": query_context.result_format.value,
        "result_type": query_context.result_type.value,
    }


def serialize_query_context_for_artifact(query_context: Any) -> dict[str, Any]:
    """Return a Celery JSON-safe chart-data request payload."""
    payload = _query_context_payload(query_context)
    return json.loads(json.dumps(payload, default=json.json_iso_dttm_ser))


def _materialize_export(payload: dict[str, Any]) -> tuple[bytes, str]:
    query_context = ChartDataQueryContextSchema().load(payload)
    if query_context.result_format != ChartDataResultFormat.CSV:
        raise ValueError("Chart-data artifacts support CSV exports only")
    g.form_data = payload
    command = ChartDataCommand(query_context)
    command.validate()
    result = command.execute().materialize()
    if query_context.result_type == ChartDataResultType.POST_PROCESSED:
        result = apply_client_processing(
            result,
            query_context.form_data,
            query_context.datasource,
        )
    queries = result["queries"]
    if not queries:
        raise ValueError("Cannot create an artifact from an empty query result")

    encoding = current_app.config["CSV_EXPORT"].get("encoding", "utf-8")

    def as_bytes(data: Any) -> bytes:
        return data if isinstance(data, bytes) else str(data).encode(encoding)

    if len(queries) == 1:
        return as_bytes(queries[0]["data"]), "text/csv"
    files = {
        f"query_{index + 1}.csv": as_bytes(query["data"])
        for index, query in enumerate(queries)
    }
    return create_zip(files).getvalue(), "application/zip"


def _tracked_chunks(
    content: bytes,
    digest: Any,
    aborted: threading.Event,
) -> Iterable[bytes]:
    for offset in range(0, len(content), _WRITE_CHUNK_SIZE):
        if aborted.is_set():
            raise RuntimeError("Chart-data export was cancelled")
        chunk = content[offset : offset + _WRITE_CHUNK_SIZE]
        digest.update(chunk)
        yield chunk


def _artifact_expiration() -> datetime:
    """Return the cleanup deadline for the artifact's current lifecycle phase."""
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(
        seconds=current_app.config["CHART_DATA_ARTIFACT_TTL_SECONDS"]
    )


def _artifact_filename(filename: str, extension: str) -> str:
    """Return a safe filename bounded by the metadata column width."""
    filename_stem = secure_filename(Path(filename).stem) or "export"
    filename_stem = filename_stem[: _MAX_ARTIFACT_FILENAME_LENGTH - len(extension)]
    return f"{filename_stem}{extension}"


@transaction()
def _create_artifact_tombstone(artifact: ChartDataExportArtifact) -> None:
    """Commit the cleanup key before external object storage is mutated."""
    ChartDataExportArtifactDAO.create(item=artifact)


@transaction()
def _finalize_artifact_metadata(
    artifact: ChartDataExportArtifact,
    size: int,
    sha256: str,
    filename: str,
    content_type: str,
    expires_at: datetime,
) -> None:
    """Commit metadata only after the artifact object is durable."""
    artifact.size_bytes = size
    artifact.sha256 = sha256
    artifact.filename = filename
    artifact.content_type = content_type
    artifact.expires_at = expires_at


@task(name="chart_data.generate_export_artifact", scope=TaskScope.PRIVATE)
def generate_chart_data_export_artifact(
    payload: dict[str, Any],
    owner_id: int,
    filename: str,
) -> None:
    """Generate one private downloadable artifact under the initiating user."""
    context = get_context()
    task_record = TaskDAO.find_one_or_none(
        uuid=context.task_uuid,
        skip_base_filter=True,
    )
    user = security_manager.get_user_by_id(owner_id)
    if task_record is None or user is None or task_record.user_id != owner_id:
        raise RuntimeError("Artifact task ownership could not be verified")

    store = get_chart_data_artifact_store()
    storage_key = str(uuid4())
    aborted = threading.Event()

    @context.on_abort
    def abort_export() -> None:
        aborted.set()

    @context.on_cleanup
    def cleanup_incomplete_export() -> None:
        # Status transitions use bulk updates with synchronize_session=False.
        # Read the scalar status so an identity-mapped IN_PROGRESS Task cannot
        # make cleanup remove an artifact after a successful transition.
        if TaskDAO.get_status(context.task_uuid) == TaskStatus.SUCCESS.value:
            return
        artifact = ChartDataExportArtifactDAO.find_by_task_id(task_record.id)
        if artifact is not None:
            delete_chart_data_export_artifact_transactionally(artifact)
        else:
            store.delete(storage_key)

    datasource = payload["datasource"]
    artifact = ChartDataExportArtifact(
        task_id=task_record.id,
        owner_id=owner_id,
        datasource_id=str(datasource["id"]),
        datasource_type=str(datasource["type"]),
        storage_key=storage_key,
        filename=_artifact_filename(filename, ".csv"),
        content_type=_INCOMPLETE_ARTIFACT_CONTENT_TYPE,
        size_bytes=0,
        sha256="",
        expires_at=_artifact_expiration(),
    )
    with override_user(user):
        _create_artifact_tombstone(artifact)

    with override_user(user):
        content, content_type = _materialize_export(payload)
    if aborted.is_set():
        raise RuntimeError("Chart-data export was cancelled")

    extension = ".zip" if content_type == "application/zip" else ".csv"
    artifact_filename = _artifact_filename(filename, extension)
    digest = hashlib.sha256()
    size = store.write(storage_key, _tracked_chunks(content, digest, aborted))
    if aborted.is_set():
        raise RuntimeError("Chart-data export was cancelled")
    expires_at = _artifact_expiration()
    _finalize_artifact_metadata(
        artifact,
        size,
        digest.hexdigest(),
        artifact_filename,
        content_type,
        expires_at,
    )
    context.update_task(
        progress=1.0,
        payload={
            "artifact_uuid": str(artifact.uuid),
            "content_type": content_type,
            "expires_at": expires_at.isoformat(),
            "filename": artifact_filename,
            "sha256": artifact.sha256,
            "size_bytes": size,
        },
    )
