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
import csv
import io
import logging
import math
import time
from abc import abstractmethod
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from numbers import Integral, Real
from typing import Any, Callable, Generator

from flask import current_app as app, g, has_app_context
from sqlalchemy import text

from superset import db
from superset.commands.base import BaseCommand
from superset.extensions import event_logger, security_manager
from superset.utils import json
from superset.utils.csv import escape_value

logger = logging.getLogger(__name__)


@contextmanager
def preserve_g_context(
    captured_g: dict[str, Any],
) -> Generator[None, None, None]:
    """
    Context manager that restores captured flask.g attributes.

    This is needed for streaming responses where the generator runs in a new
    app context but needs access to request-scoped data from the original request.

    Args:
        captured_g: Dictionary of g attributes captured before context switch
    """
    for key, value in captured_g.items():
        setattr(g, key, value)
    yield


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
        self._current_app = app._get_current_object()

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

    @staticmethod
    def _csv_writer_options(config: dict[str, Any]) -> dict[str, Any]:
        key_map = {
            "sep": "delimiter",
            "quotechar": "quotechar",
            "quoting": "quoting",
            "lineterminator": "lineterminator",
            "doublequote": "doublequote",
            "escapechar": "escapechar",
        }
        options = {
            writer_key: config[config_key]
            for config_key, writer_key in key_map.items()
            if config_key in config
        }
        options.setdefault("lineterminator", "\n")
        return options

    @classmethod
    def supports_csv_export_config(cls, config: dict[str, Any]) -> bool:
        """Return whether the incremental writer can reproduce this config."""
        if not set(config).issubset(cls._supported_csv_export_keys):
            return False
        decimal = config.get("decimal", ".")
        if not isinstance(decimal, str) or len(decimal) != 1:
            return False
        if (date_format := config.get("date_format")) is not None and not isinstance(
            date_format, str
        ):
            return False
        float_format = config.get("float_format")
        if float_format is not None and not (
            isinstance(float_format, str) or callable(float_format)
        ):
            return False
        try:
            codecs.lookup(str(config.get("encoding", "utf-8")))
            csv.writer(io.StringIO(), **cls._csv_writer_options(config))
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

    def _write_csv_header(
        self, columns: list[str], csv_writer: Any, buffer: io.StringIO
    ) -> tuple[str, int]:
        """Write CSV header and return header data with byte count."""
        csv_writer.writerow([escape_value(column) for column in columns])
        header_data = buffer.getvalue()
        total_bytes = len(header_data.encode("utf-8"))
        buffer.seek(0)
        buffer.truncate()
        return header_data, total_bytes

    @staticmethod
    def _format_value(
        value: Any,
        decimal_separator: str | None,
        csv_export_config: dict[str, Any],
    ) -> Any:
        na_rep = escape_value(str(csv_export_config.get("na_rep", "")))
        if value is None:
            return na_rep
        if isinstance(value, (dict, list)):
            return json.dumps(value, default=json.json_int_dttm_ser)
        if (
            isinstance(value, (Decimal, Real))
            and not isinstance(value, bool)
            and math.isinf(value)
        ):
            return na_rep
        if (
            (date_format := csv_export_config.get("date_format"))
            and isinstance(value, (date, datetime))
        ):
            return escape_value(value.strftime(str(date_format)))
        if (
            (float_format := csv_export_config.get("float_format"))
            and isinstance(value, Real)
            and not isinstance(value, (bool, Integral))
        ):
            value = (
                float_format(value)
                if callable(float_format)
                else str(float_format) % value
            )
        if isinstance(value, str):
            if decimal_separator and decimal_separator != ".":
                value = value.replace(".", decimal_separator)
            return escape_value(value)
        if (
            decimal_separator
            and decimal_separator != "."
            and isinstance(value, (float, Decimal, Real))
            and not isinstance(value, bool)
        ):
            return str(value).replace(".", decimal_separator)
        return value

    def _format_row_values(
        self,
        row: tuple[Any, ...],
        decimal_separator: str | None,
        column_mutators: dict[int, Callable[[Any], Any]],
        csv_export_config: dict[str, Any],
    ) -> list[Any]:
        """Normalize and format one database row for incremental CSV output."""
        return [
            self._format_value(
                mutator(value) if (mutator := column_mutators.get(index)) else value,
                decimal_separator,
                csv_export_config,
            )
            for index, value in enumerate(row)
        ]

    def _get_output_columns(self, result_proxy: Any) -> list[str]:
        """Return public columns in their final CSV order."""
        return list(result_proxy.keys())

    @staticmethod
    def _get_column_mutators(
        result_proxy: Any, database: Any
    ) -> dict[int, Callable[[Any], Any]]:
        """Resolve the same engine-specific value mutators as fetch_data()."""
        cursor = getattr(result_proxy, "cursor", None)
        description = getattr(cursor, "description", None) or []
        engine_spec = database.db_engine_spec
        mutators: dict[int, Callable[[Any], Any]] = {}
        for index, column in enumerate(description):
            sqla_type = engine_spec.get_sqla_column_type(
                engine_spec.get_datatype(column[1])
            )
            if mutator := engine_spec.column_type_mutators.get(type(sqla_type)):
                mutators[index] = mutator
        return mutators

    def _emit_stream_error_marker(self) -> bool:
        """Whether legacy clients require an in-band stream error marker."""
        return True

    def _process_rows(
        self,
        result_proxy: Any,
        csv_writer: Any,
        buffer: io.StringIO,
        limit: int | None,
        decimal_separator: str | None = None,
        column_count: int | None = None,
        column_mutators: dict[int, Callable[[Any], Any]] | None = None,
        csv_export_config: dict[str, Any] | None = None,
    ) -> Generator[tuple[str, int, int], None, None]:
        """
        Process database rows and yield CSV data chunks.

        Args:
            result_proxy: SQLAlchemy result proxy
            csv_writer: CSV writer instance
            buffer: StringIO buffer for CSV data
            limit: Maximum number of rows to process, or None for unlimited
            decimal_separator: Custom decimal separator (e.g., ",") or None

        Yields tuples of (data_chunk, row_count, byte_count).
        """
        row_count = 0
        flush_threshold = 65536  # 64KB

        while rows := result_proxy.fetchmany(self._chunk_size):
            for row in rows:
                # Apply limit if specified
                if limit is not None and row_count >= limit:
                    break

                # Format values with custom decimal separator if needed
                output_row = row[:column_count] if column_count is not None else row
                formatted_row = self._format_row_values(
                    output_row,
                    decimal_separator,
                    column_mutators or {},
                    csv_export_config or {},
                )
                csv_writer.writerow(formatted_row)
                row_count += 1

                # Check buffer size and flush if needed
                current_size = buffer.tell()
                if current_size >= flush_threshold:
                    data = buffer.getvalue()
                    data_bytes = len(data.encode("utf-8"))
                    yield data, row_count, data_bytes
                    buffer.seek(0)
                    buffer.truncate()

            # Break outer loop if limit reached
            if limit is not None and row_count >= limit:
                break

        # Flush remaining buffer
        if remaining_data := buffer.getvalue():
            data_bytes = len(remaining_data.encode("utf-8"))
            yield remaining_data, row_count, data_bytes

    def _execute_query_and_stream(
        self,
        sql: str,
        database: Any,
        limit: int | None,
        catalog: str | None = None,
        schema: str | None = None,
    ) -> Generator[str, None, None]:
        """Execute query with streaming and yield CSV chunks."""
        start_time = time.time()
        total_bytes = 0

        # Get CSV export configuration. CSV_EXPORT has an explicit default in
        # config.py, so index directly rather than using .get() with a hardcoded
        # fallback that would silently mask a misconfiguration removing the key.
        #
        # The streaming path only honors the `sep` and `decimal` keys from
        # CSV_EXPORT. Unlike the non-streaming path in
        # superset.charts.client_processing (which builds the whole file with a
        # single DataFrame.to_csv(**CSV_EXPORT) call), this path writes rows
        # incrementally via csv.writer, so the remaining pandas to_csv kwargs
        # (e.g. quotechar, lineterminator, encoding) do not map onto it and are
        # intentionally not applied here.
        csv_export_config = app.config["CSV_EXPORT"]
        decimal_separator = csv_export_config.get("decimal", ".")

        with db.session() as session:
            # Merge database to prevent DetachedInstanceError
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

                    columns = self._get_output_columns(result_proxy)
                    column_mutators = self._get_column_mutators(
                        result_proxy, merged_database
                    )

                    # Use StringIO with csv.writer for proper escaping
                    # Apply delimiter from CSV_EXPORT config
                    buffer = io.StringIO()
                    csv_writer = csv.writer(
                        buffer, **self._csv_writer_options(csv_export_config)
                    )

                    # Write CSV header
                    header_data, header_bytes = self._write_csv_header(
                        columns, csv_writer, buffer
                    )
                    total_bytes += header_bytes
                    yield header_data

                    # Process rows and yield chunks
                    row_count = 0
                    for data_chunk, rows_processed, chunk_bytes in self._process_rows(
                        result_proxy,
                        csv_writer,
                        buffer,
                        limit,
                        decimal_separator,
                        len(columns),
                        column_mutators,
                        csv_export_config,
                    ):
                        total_bytes += chunk_bytes
                        row_count = rows_processed
                        yield data_chunk

                    # Log completion
                    total_time = time.time() - start_time
                    total_mb = total_bytes / (1024 * 1024)
                    logger.info(
                        "Streaming CSV completed: %s rows, %.1fMB in %.2fs",
                        f"{row_count:,}",
                        total_mb,
                        total_time,
                    )

    def run(self) -> Callable[[], Generator[str, None, None]]:
        """
        Execute the streaming CSV export.

        Returns:
            A callable that returns a generator yielding CSV data chunks as strings.
            The callable is needed to maintain Flask app context during streaming.
        """
        # Load all needed data while session is still active
        # to avoid DetachedInstanceError
        sql, database, catalog, schema = self._get_sql_and_database()
        limit = self._get_row_limit()
        # Capture flask.g attributes to preserve request-scoped data
        # when the streaming generator runs in a new app context.
        captured_g = (
            g._get_current_object().__dict__.copy() if has_app_context() else {}
        )

        def csv_generator() -> Generator[str, None, None]:
            """Generator that yields CSV data chunks."""
            with self._current_app.app_context():
                with preserve_g_context(captured_g):
                    try:
                        yield from self._execute_query_and_stream(
                            sql, database, limit, catalog, schema
                        )
                    except Exception as e:
                        logger.exception("Error in streaming CSV generator: %s", e)
                        if self._emit_stream_error_marker():
                            yield (
                                "__STREAM_ERROR__:Export failed. "
                                "Please try again in some time.\n"
                            )
                        else:
                            raise

        return csv_generator
