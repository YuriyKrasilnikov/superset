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
import tempfile
import threading
import zipfile
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, BinaryIO, Iterable
from uuid import uuid4

from flask import current_app, g
from superset_core.tasks.types import TaskScope
from werkzeug.utils import secure_filename

from superset import security_manager
from superset.charts.client_processing import apply_client_processing
from superset.charts.data.artifacts import (
    ChartDataArtifactStore,
    delete_chart_data_export_artifact_transactionally,
    get_chart_data_artifact_store,
)
from superset.charts.schemas import ChartDataQueryContextSchema
from superset.commands.chart.data.get_data_command import ChartDataCommand
from superset.common.chart_data import ChartDataResultFormat, ChartDataResultType
from superset.daos.chart_data_export_artifact import ChartDataExportArtifactDAO
from superset.daos.tasks import TaskDAO
from superset.models.chart_data_export_artifact import (
    ChartDataExportArtifact,
    ChartDataExportArtifactState,
)
from superset.tasks.ambient_context import get_context
from superset.tasks.context import TaskContext
from superset.tasks.decorators import task
from superset.utils import json
from superset.utils.core import override_user
from superset.utils.decorators import transaction

_WRITE_CHUNK_SIZE = 1024 * 1024
_MAX_ARTIFACT_FILENAME_LENGTH = 255
_INCOMPLETE_ARTIFACT_CONTENT_TYPE = "application/octet-stream"


@dataclass(frozen=True)
class PreparedArtifactPublication:
    """Metadata for an object durably written but not yet published."""

    size_bytes: int
    sha256: str
    filename: str
    content_type: str
    expires_at: datetime


@dataclass
class ArtifactPublicationLifecycle:
    """Coordinate abort, fenced publication, and compensating cleanup."""

    context: TaskContext
    artifact: ChartDataExportArtifact
    store: ChartDataArtifactStore
    aborted: threading.Event = field(default_factory=threading.Event)
    ready: threading.Event = field(default_factory=threading.Event)
    publication: PreparedArtifactPublication | None = None

    def abort(self) -> None:
        self.aborted.set()

    def cleanup(self) -> None:
        if self.ready.is_set() and not self.aborted.is_set():
            return
        stored_artifact = ChartDataExportArtifactDAO.find_by_uuid(self.artifact.uuid)
        if (
            stored_artifact is not None
            and stored_artifact.storage_key == self.artifact.storage_key
        ):
            delete_chart_data_export_artifact_transactionally(stored_artifact)
        else:
            self.store.delete(self.artifact.storage_key)

    def publish(self) -> None:
        if self.publication is None or self.aborted.is_set():
            raise RuntimeError("Artifact was not prepared for publication")
        _publish_artifact_metadata(self.artifact, self.publication)
        self.context.update_task(
            progress=1.0,
            payload={
                "artifact_uuid": str(self.artifact.uuid),
                "content_type": self.publication.content_type,
                "expires_at": self.publication.expires_at.isoformat(),
                "filename": self.publication.filename,
                "sha256": self.publication.sha256,
                "size_bytes": self.publication.size_bytes,
            },
        )
        self.ready.set()


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


def _materialize_export(payload: dict[str, Any]) -> tuple[bytes | BinaryIO, str]:
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

    archive = tempfile.TemporaryFile(mode="w+b")
    try:
        with zipfile.ZipFile(
            archive,
            mode="w",
            compression=zipfile.ZIP_DEFLATED,
        ) as zip_file:
            for index, query in enumerate(queries):
                zip_file.writestr(
                    f"query_{index + 1}.csv",
                    as_bytes(query["data"]),
                )
                query["data"] = None
        archive.seek(0)
        return archive, "application/zip"
    except BaseException:
        archive.close()
        raise


def _tracked_chunks(
    content: bytes | BinaryIO,
    digest: Any,
    aborted: threading.Event,
) -> Iterable[bytes]:
    chunks: Iterable[bytes]
    if isinstance(content, bytes):
        chunks = (
            content[offset : offset + _WRITE_CHUNK_SIZE]
            for offset in range(0, len(content), _WRITE_CHUNK_SIZE)
        )
    else:
        chunks = iter(lambda: content.read(_WRITE_CHUNK_SIZE), b"")
    for chunk in chunks:
        if aborted.is_set():
            return
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
def _publish_artifact_metadata(
    artifact: ChartDataExportArtifact,
    publication: PreparedArtifactPublication,
) -> None:
    """Atomically publish metadata for the worker's creating tombstone."""
    if not ChartDataExportArtifactDAO.mark_ready(
        artifact.uuid,
        artifact.storage_key,
        size_bytes=publication.size_bytes,
        sha256=publication.sha256,
        filename=publication.filename,
        content_type=publication.content_type,
        expires_at=publication.expires_at,
    ):
        raise RuntimeError("Artifact publication fence was lost")


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

    datasource = payload["datasource"]
    artifact = ChartDataExportArtifact(
        uuid=uuid4(),
        task_id=task_record.id,
        owner_id=owner_id,
        datasource_id=str(datasource["id"]),
        datasource_type=str(datasource["type"]),
        storage_key=storage_key,
        state=ChartDataExportArtifactState.CREATING.value,
        filename=_artifact_filename(filename, ".csv"),
        content_type=_INCOMPLETE_ARTIFACT_CONTENT_TYPE,
        size_bytes=0,
        sha256="",
        expires_at=_artifact_expiration(),
    )
    lifecycle = ArtifactPublicationLifecycle(context, artifact, store)
    context.on_abort(lifecycle.abort)
    context.on_cleanup(lifecycle.cleanup)
    context.on_finalize(lifecycle.publish)
    with override_user(user):
        _create_artifact_tombstone(artifact)

    if lifecycle.aborted.is_set():
        return
    with override_user(user):
        content, content_type = _materialize_export(payload)
    try:
        if lifecycle.aborted.is_set():
            return

        extension = ".zip" if content_type == "application/zip" else ".csv"
        artifact_filename = _artifact_filename(filename, extension)
        digest = hashlib.sha256()
        size = store.write(
            storage_key,
            _tracked_chunks(content, digest, lifecycle.aborted),
        )
        if lifecycle.aborted.is_set():
            return
    finally:
        if not isinstance(content, bytes):
            content.close()
    lifecycle.publication = PreparedArtifactPublication(
        size_bytes=size,
        sha256=digest.hexdigest(),
        filename=artifact_filename,
        content_type=content_type,
        expires_at=_artifact_expiration(),
    )
