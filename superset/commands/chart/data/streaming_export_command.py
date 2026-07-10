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
"""Command for streaming CSV exports of chart data."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any, cast, Protocol, TYPE_CHECKING

from flask import current_app

from superset.commands.streaming_export.base import BaseStreamingCSVExportCommand
from superset.common.chart_data import ChartDataResultFormat, ChartDataResultType
from superset.exceptions import QueryObjectValidationError
from superset.models.helpers import QueryStringExtended
from superset.sql.parse import SQLScript
from superset.superset_typing import QueryObjectDict

if TYPE_CHECKING:
    from superset.common.query_context import QueryContext
    from superset.common.query_object import QueryObject


class StreamingExportIneligibility(str, Enum):
    """Stable reasons why a chart export must use a non-direct path."""

    NOT_CSV = "not_csv"
    RESULT_TRANSFORM = "result_transform"
    MULTIPLE_QUERIES = "multiple_queries"
    QUERY_DEPENDENCY = "query_dependency"
    VALUE_NORMALIZATION = "value_normalization"
    UNSUPPORTED_DATASOURCE = "unsupported_datasource"
    UNSUPPORTED_ENGINE = "unsupported_engine"
    UNSUPPORTED_DIALECT = "unsupported_dialect"
    MULTI_STATEMENT_SQL = "multi_statement_sql"
    CSV_CONFIGURATION = "csv_configuration"


@dataclass(frozen=True)
class PreparedStreamingQuery:
    """Final SQL and output contract proven eligible for direct streaming."""

    sql: str
    database: Any
    catalog: str | None
    schema: str | None
    labels: tuple[str, ...]


@dataclass(frozen=True)
class StreamingExportPreparation:
    """Result of direct-streaming planning without executing the main query."""

    query: PreparedStreamingQuery | None = None
    ineligibility: StreamingExportIneligibility | None = None

    @property
    def eligible(self) -> bool:
        return self.query is not None


class SQLStreamingDatasource(Protocol):
    """Datasource capabilities required to compile a direct export."""

    database: Any
    catalog: str | None
    schema: str | None

    def get_query_str_extended(
        self,
        query_obj: QueryObjectDict,
        mutate: bool = True,
        defer_source_queries: bool = False,
    ) -> QueryStringExtended: ...

    def _raise_for_disallowed_sql(self, sql: str) -> None: ...

    def _collect_dttm_labels(
        self, query_object: QueryObject
    ) -> tuple[tuple[str, str | None], ...]: ...


class StreamingCSVExportCommand(BaseStreamingCSVExportCommand):
    """
    Command to execute a streaming CSV export for chart data.

    This command handles chart-specific logic:
    - QueryContext validation
    - Datasource preparation and SQL generation
    - No row limit (exports all chart data)
    """

    def __init__(
        self,
        query_context: QueryContext,
        chunk_size: int = 1000,
    ):
        """
        Initialize the chart streaming export command.

        Args:
            query_context: The query context containing datasource and query details
            chunk_size: Number of rows to fetch per database query (default: 1000)
        """
        super().__init__(chunk_size)
        self._query_context = query_context
        self._prepared: PreparedStreamingQuery | None = None
        self._validated = False

    def validate(self) -> None:
        """Validate permissions and query context."""
        if not self._validated:
            self._query_context.raise_for_access()
            self._validated = True

    @staticmethod
    def _has_python_result_transform(query: QueryObject) -> bool:
        return bool(
            query.post_processing
            or query.time_offsets
            or query.annotation_layers
            or query.is_rowcount
        )

    def _semantic_ineligibility(self) -> StreamingExportIneligibility | None:
        if self._query_context.result_format != ChartDataResultFormat.CSV:
            return StreamingExportIneligibility.NOT_CSV
        if self._query_context.result_type != ChartDataResultType.FULL:
            return StreamingExportIneligibility.RESULT_TRANSFORM
        if len(self._query_context.queries) != 1:
            return StreamingExportIneligibility.MULTIPLE_QUERIES

        query_obj = self._query_context.queries[0]
        if self._has_python_result_transform(query_obj):
            return StreamingExportIneligibility.RESULT_TRANSFORM
        if query_obj.contribution_totals_query_index is not None:
            return StreamingExportIneligibility.QUERY_DEPENDENCY
        return None

    def _get_sql_datasource(self) -> SQLStreamingDatasource | None:
        datasource = self._query_context.datasource
        if not (
            callable(getattr(datasource, "get_query_str_extended", None))
            and callable(getattr(datasource, "_raise_for_disallowed_sql", None))
            and getattr(datasource, "database", None) is not None
        ):
            return None
        return cast(SQLStreamingDatasource, datasource)

    @staticmethod
    def _engine_ineligibility(
        datasource: SQLStreamingDatasource,
    ) -> StreamingExportIneligibility | None:
        database = datasource.database
        if not database.db_engine_spec.supports_direct_csv_streaming:
            return StreamingExportIneligibility.UNSUPPORTED_ENGINE
        with database.get_sqla_engine(
            catalog=datasource.catalog,
            schema=datasource.schema,
        ) as engine:
            if not bool(getattr(engine.dialect, "supports_server_side_cursors", False)):
                return StreamingExportIneligibility.UNSUPPORTED_DIALECT
        return None

    def prepare(self) -> StreamingExportPreparation:
        """Compile and validate a semantically direct, server-streamable query."""
        if self._prepared is not None:
            return StreamingExportPreparation(query=self._prepared)

        self.validate()
        if reason := self._semantic_ineligibility():
            return StreamingExportPreparation(ineligibility=reason)
        if not self.supports_csv_export_config(current_app.config["CSV_EXPORT"]):
            return StreamingExportPreparation(
                ineligibility=StreamingExportIneligibility.CSV_CONFIGURATION
            )
        query_obj = self._query_context.queries[0]
        query_obj.validate()

        if (sql_datasource := self._get_sql_datasource()) is None:
            return StreamingExportPreparation(
                ineligibility=StreamingExportIneligibility.UNSUPPORTED_DATASOURCE
            )
        if (
            query_obj.metrics
            or query_obj.is_timeseries
            or sql_datasource._collect_dttm_labels(query_obj)
        ):
            return StreamingExportPreparation(
                ineligibility=StreamingExportIneligibility.VALUE_NORMALIZATION
            )
        database = sql_datasource.database
        if reason := self._engine_ineligibility(sql_datasource):
            return StreamingExportPreparation(ineligibility=reason)

        query_str_ext = sql_datasource.get_query_str_extended(
            query_obj.to_dict(),
            mutate=True,
            defer_source_queries=False,
        )
        sql = database.mutate_sql_based_on_config(
            query_str_ext.sql,
            is_split=True,
        )
        script = SQLScript(sql, engine=database.db_engine_spec.engine)
        if len(script.statements) != 1:
            return StreamingExportPreparation(
                ineligibility=StreamingExportIneligibility.MULTI_STATEMENT_SQL
            )
        sql_datasource._raise_for_disallowed_sql(sql)
        self._prepared = PreparedStreamingQuery(
            sql=sql,
            database=database,
            catalog=sql_datasource.catalog,
            schema=sql_datasource.schema,
            labels=tuple(query_str_ext.labels_expected),
        )
        return StreamingExportPreparation(query=self._prepared)

    def _get_sql_and_database(self) -> tuple[str, Any, str | None, str | None]:
        """
        Get the SQL query, database, catalog, and schema for chart export.

        Returns:
            Tuple of (sql_query, database_object, catalog, schema)
        """
        preparation = self.prepare()
        if preparation.query is None:
            reason = preparation.ineligibility or "unknown"
            raise ValueError(f"Chart query is not eligible for direct export: {reason}")
        prepared = preparation.query
        return prepared.sql, prepared.database, prepared.catalog, prepared.schema

    def _get_output_columns(self, result_proxy: Any) -> list[str]:
        preparation = self.prepare()
        if preparation.query is None or not preparation.query.labels:
            return super()._get_output_columns(result_proxy)
        raw_columns = list(result_proxy.keys())
        labels = list(preparation.query.labels)
        if len(raw_columns) < len(labels):
            raise QueryObjectValidationError(
                "Database did not return all columns required by the export"
            )
        return labels

    def _emit_stream_error_marker(self) -> bool:
        # A marker is valid neither CSV nor a reliable transport status. Let the
        # WSGI server terminate the response so fetch() observes a stream failure.
        return False

    def _get_row_limit(self) -> int | None:
        """
        Get the row limit for chart export.

        Returns:
            None (no limit for chart exports)
        """
        return None
