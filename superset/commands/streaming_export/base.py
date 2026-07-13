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
"""Base command for streaming CSV exports."""

from __future__ import annotations

import codecs
import logging
import time
from abc import abstractmethod
from typing import Any, Generator

import pandas as pd
from flask import current_app as app
from sqlalchemy import text

from superset import db
from superset.commands.base import BaseCommand
from superset.exceptions import QueryObjectValidationError
from superset.extensions import event_logger, security_manager
from superset.result_set import (
    dedup,
    normalize_cursor_description_names,
    SupersetResultSet,
)
from superset.superset_typing import DbapiDescription
from superset.utils import csv as csv_utils

logger = logging.getLogger(__name__)


class BaseStreamingCSVExportCommand(BaseCommand):
    """
    Base class for streaming CSV export commands.

    Provides shared functionality for:
    - Generating CSV data in chunks
    - Managing database connections
    - Buffering data for efficient streaming
    - Error handling with user-friendly messages

    Subclasses must implement:
    - _get_sql_and_database(): Return SQL query string and database object
    - _get_row_limit(): Return optional row limit for the export
    """

    def __init__(self, chunk_size: int = 1000):
        """
        Initialize the streaming export command.

        Args:
            chunk_size: Number of rows to fetch per database query (default: 1000)
        """
        self._chunk_size = chunk_size
        self._rows_streamed = 0

    _supported_csv_export_keys = frozenset(
        {
            "date_format",
            "decimal",
            "doublequote",
            "encoding",
            "escapechar",
            "float_format",
            "lineterminator",
            "na_rep",
            "quotechar",
            "quoting",
            "sep",
        }
    )

    @classmethod
    def supports_csv_export_config(cls, config: dict[str, Any]) -> bool:
        """Return whether bounded pandas chunks can reproduce this config."""
        if not set(config).issubset(cls._supported_csv_export_keys):
            return False
        try:
            codecs.lookup(str(config.get("encoding", "utf-8")))
            csv_utils.df_to_escaped_csv(
                pd.DataFrame(),
                index=False,
                **config,
            )
        except (LookupError, TypeError, ValueError):
            return False
        return True

    @abstractmethod
    def _get_sql_and_database(self) -> tuple[str, Any, str | None, str | None]:
        """
        Get the SQL query, database, catalog, and schema for execution.

        Returns:
            Tuple of (sql_query, database_object, catalog, schema)
        """

    @abstractmethod
    def _get_row_limit(self) -> int | None:
        """
        Get the row limit for the export.

        Returns:
            Row limit or None for unlimited
        """

    @staticmethod
    def _get_cursor_description(result_proxy: Any) -> DbapiDescription:
        """Return a concrete DBAPI description, falling back to result keys."""
        cursor = getattr(result_proxy, "cursor", None)
        description = getattr(cursor, "description", None)
        try:
            concrete_description = list(description or ())
        except TypeError:
            concrete_description = []
        if concrete_description:
            return concrete_description
        return [
            (str(column), "", None, None, None, None, False)
            for column in result_proxy.keys()
        ]

    def _get_output_columns(self, result_proxy: Any) -> list[str]:
        """Return public columns in their final CSV order."""
        description = self._get_cursor_description(result_proxy)
        return dedup(normalize_cursor_description_names(description))

    def _normalize_dataframe(
        self,
        dataframe: pd.DataFrame,
        database: Any,
        output_columns: list[str],
    ) -> pd.DataFrame:
        """Apply the canonical materialized-result normalization boundary."""
        dataframe = database.post_process_df(dataframe)
        if len(dataframe.columns) < len(output_columns):
            raise ValueError("Database returned fewer columns than the CSV contract")
        dataframe = dataframe.iloc[:, : len(output_columns)].copy()
        dataframe.columns = output_columns
        return dataframe

    def _dataframe_from_rows(
        self,
        rows: list[Any],
        result_proxy: Any,
        database: Any,
        output_columns: list[str],
    ) -> pd.DataFrame:
        """Build one bounded DataFrame through Superset's canonical result set."""
        description = self._get_cursor_description(result_proxy)
        dataframe = SupersetResultSet(
            rows,
            description,
            database.db_engine_spec,
        ).to_pandas_df()
        return self._normalize_dataframe(dataframe, database, output_columns)

    def _emit_stream_error_marker(self) -> bool:
        """Whether legacy clients require an in-band stream error marker."""
        return True

    def _process_rows(
        self,
        result_proxy: Any,
        database: Any,
        output_columns: list[str],
        csv_export_config: dict[str, Any],
        limit: int | None,
    ) -> Generator[str, None, None]:
        """Normalize and serialize bounded row chunks without semantic drift."""
        row_count = 0
        while limit is None or row_count < limit:
            remaining = None if limit is None else max(limit - row_count, 0)
            fetch_size = (
                self._chunk_size
                if remaining is None
                else min(self._chunk_size, remaining)
            )
            rows = result_proxy.fetchmany(fetch_size)
            if not rows:
                break
            bounded_rows = list(rows if remaining is None else rows[:remaining])
            dataframe = self._dataframe_from_rows(
                bounded_rows,
                result_proxy,
                database,
                output_columns,
            )
            row_count += len(dataframe.index)
            self._rows_streamed = row_count
            yield csv_utils.df_to_escaped_csv(
                dataframe,
                index=False,
                header=False,
                **csv_export_config,
            )

    def _execute_query_and_stream(
        self,
        sql: str,
        database: Any,
        limit: int | None,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> Generator[str, None, None]:
        """Execute query with streaming and yield CSV chunks."""
        start_time = time.perf_counter()
        csv_export_config = app.config["CSV_EXPORT"]

        with db.session() as session:
            merged_database = session.merge(database)
            with merged_database.get_sqla_engine(
                catalog=catalog, schema=schema
            ) as engine:
                with engine.connect() as connection:
                    if query_logger := app.config["QUERY_LOGGER"]:
                        query_logger(
                            engine.url,
                            sql,
                            schema,
                            __name__,
                            security_manager,
                        )
                    with event_logger.log_context(
                        action="execute_sql",
                        database=merged_database,
                        object_ref=__name__,
                    ):
                        result_proxy = connection.execution_options(
                            stream_results=True
                        ).execute(text(sql))
                        output_columns = self._get_output_columns(result_proxy)
                        yield csv_utils.df_to_escaped_csv(
                            pd.DataFrame(columns=output_columns),
                            index=False,
                            **csv_export_config,
                        )
                        yield from self._process_rows(
                            result_proxy,
                            merged_database,
                            output_columns,
                            csv_export_config,
                            limit,
                        )

        logger.info(
            "Streaming CSV query completed: %s rows in %.2fs",
            f"{self._rows_streamed:,}",
            time.perf_counter() - start_time,
        )

    def run(self) -> Generator[bytes, None, None]:
        """
        Execute the streaming CSV export.

        Returns:
            Encoded CSV chunks. The response owner must keep the Flask request
            context active while iterating them.
        """
        csv_export_config = app.config["CSV_EXPORT"]
        if not self.supports_csv_export_config(csv_export_config):
            raise QueryObjectValidationError(
                "CSV_EXPORT contains options unsupported by bounded streaming"
            )
        sql, database, catalog, schema = self._get_sql_and_database()
        limit = self._get_row_limit()
        encoding = str(csv_export_config.get("encoding", "utf-8"))

        def csv_generator() -> Generator[bytes, None, None]:
            """Encode the entire stream with one stateful codec instance."""
            encoder = codecs.getincrementalencoder(encoding)()
            total_bytes = 0
            started = time.perf_counter()
            try:
                for chunk in self._execute_query_and_stream(
                    sql, database, limit, catalog, schema
                ):
                    encoded = encoder.encode(chunk)
                    total_bytes += len(encoded)
                    if encoded:
                        yield encoded
                if final_chunk := encoder.encode("", final=True):
                    total_bytes += len(final_chunk)
                    yield final_chunk
            except Exception as ex:
                logger.exception("Error in streaming CSV generator: %s", ex)
                if not self._emit_stream_error_marker():
                    raise
                marker = encoder.encode(
                    "__STREAM_ERROR__:Export failed. Please try again in some time.\n",
                    final=True,
                )
                total_bytes += len(marker)
                yield marker
            finally:
                logger.info(
                    "Streaming CSV response completed: %.1fMB in %.2fs",
                    total_bytes / (1024 * 1024),
                    time.perf_counter() - started,
                )

        return csv_generator()
