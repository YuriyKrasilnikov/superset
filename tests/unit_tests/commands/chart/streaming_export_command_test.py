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
"""Unit tests for Chart Streaming CSV Export Command."""

from datetime import date

import pytest
from flask import current_app
from pytest_mock import MockerFixture

from superset.commands.chart.data.streaming_export_command import (
    StreamingCSVExportCommand,
    StreamingExportIneligibility,
)
from superset.common.chart_data import ChartDataResultFormat, ChartDataResultType


def _setup_chart_mocks(
    mocker: MockerFixture,
    sql: str = "SELECT * FROM test",
    catalog: str | None = None,
    schema: str | None = None,
) -> tuple[MockerFixture, MockerFixture, MockerFixture]:
    """Set up common mocks for chart streaming export tests."""
    mock_db = mocker.patch("superset.commands.streaming_export.base.db")
    mock_session = mocker.MagicMock()
    mock_db.session.return_value.__enter__.return_value = mock_session

    query_context = mocker.MagicMock()
    query_context.result_format = ChartDataResultFormat.CSV
    query_context.result_type = ChartDataResultType.FULL
    datasource = mocker.MagicMock()
    query_str = mocker.MagicMock()
    query_str.sql = sql
    query_str.labels_expected = []
    query_str.deferred = False
    query_str.prequeries = []
    datasource.get_query_str_extended.return_value = query_str
    datasource.database = mocker.MagicMock()
    datasource.database.db_engine_spec.supports_direct_csv_streaming = True
    datasource.database.db_engine_spec.engine = "postgresql"
    datasource.database.db_engine_spec.requires_column_value_normalization = False
    datasource.database.post_process_df.side_effect = lambda dataframe: dataframe
    datasource.database.mutate_sql_based_on_config.side_effect = (
        lambda query_sql, is_split: query_sql
    )
    datasource.catalog = catalog
    datasource.schema = schema
    query_context.datasource = datasource
    query = mocker.MagicMock()
    query.post_processing = []
    query.time_offsets = []
    query.annotation_layers = []
    query.is_rowcount = False
    query.is_timeseries = False
    query.metrics = []
    query.contribution_totals_query_index = None
    query_context.queries = [query]
    datasource._collect_dttm_labels.return_value = ()
    datasource.normalize_df.side_effect = lambda dataframe, query_obj: dataframe
    mock_session.merge.return_value = datasource.database
    engine = datasource.database.get_sqla_engine.return_value.__enter__.return_value
    engine.dialect.supports_server_side_cursors = True

    return mock_db, query_context, datasource


def test_streaming_export_compiles_executable_query(mocker: MockerFixture) -> None:
    """Streaming must not execute the deferred View Query representation."""
    _, query_context, datasource = _setup_chart_mocks(
        mocker, sql="SELECT * FROM executable_query"
    )
    query = query_context.queries[0]
    query.to_dict.return_value = {"series_limit": 10}
    datasource.get_query_str.return_value = (
        "-- Main query is compiled after the series-limit prequery executes;"
    )

    sql, _, _, _ = StreamingCSVExportCommand(query_context)._get_sql_and_database()

    assert sql == "SELECT * FROM executable_query"
    datasource.get_query_str_extended.assert_called_once_with(
        {"series_limit": 10},
        mutate=True,
        defer_source_queries=True,
    )
    datasource.get_query_str.assert_not_called()


def test_streaming_export_rejects_non_sql_datasource(
    mocker: MockerFixture,
) -> None:
    """Fail before streaming when a datasource cannot compile executable SQL."""
    _, query_context, datasource = _setup_chart_mocks(mocker)
    datasource.get_query_str_extended = None

    with pytest.raises(ValueError, match="UNSUPPORTED_DATASOURCE"):
        StreamingCSVExportCommand(query_context)._get_sql_and_database()


def test_streaming_csv_export_command_init(mocker: MockerFixture) -> None:
    """Test command initialization."""
    query_context = mocker.MagicMock()
    command = StreamingCSVExportCommand(query_context, chunk_size=500)

    assert command._query_context == query_context
    assert command._chunk_size == 500


def test_streaming_csv_export_command_default_chunk_size(
    mocker: MockerFixture,
) -> None:
    """Test command uses default chunk size."""
    query_context = mocker.MagicMock()
    command = StreamingCSVExportCommand(query_context)

    assert command._chunk_size == 1000


def test_validate_calls_raise_for_access(mocker: MockerFixture) -> None:
    """Test validate method calls query context raise_for_access."""
    query_context = mocker.MagicMock()
    command = StreamingCSVExportCommand(query_context)

    command.validate()

    query_context.raise_for_access.assert_called_once()


def test_validate_raises_exception_on_access_denied(mocker: MockerFixture) -> None:
    """Test validate raises exception when access is denied."""
    query_context = mocker.MagicMock()
    query_context.raise_for_access.side_effect = Exception("Access denied")
    command = StreamingCSVExportCommand(query_context)

    with pytest.raises(Exception, match="Access denied"):
        command.validate()


def test_prepare_compiles_final_sql_without_deferred_sources(
    mocker: MockerFixture,
) -> None:
    """Planning compiles the executable main query but does not execute it."""
    _, query_context, datasource = _setup_chart_mocks(
        mocker, sql="SELECT category, SUM(value) FROM t GROUP BY category"
    )
    query = query_context.queries[0]

    preparation = StreamingCSVExportCommand(query_context).prepare()

    assert preparation.eligible
    assert preparation.query is not None
    assert preparation.query.sql == (
        "SELECT category, SUM(value) FROM t GROUP BY category"
    )
    datasource.get_query_str_extended.assert_called_once_with(
        query.to_dict.return_value,
        mutate=True,
        defer_source_queries=True,
    )
    datasource._raise_for_disallowed_sql.assert_called_once_with(preparation.query.sql)
    query_context.raise_for_access.assert_called_once_with()
    query.validate.assert_called_once_with()


@pytest.mark.parametrize(
    ("deferred", "prequeries"),
    [
        (True, []),
        (False, ["SELECT category FROM t LIMIT 10"]),
    ],
)
def test_prepare_rejects_source_query_dependencies_without_executing_them(
    mocker: MockerFixture,
    deferred: bool,
    prequeries: list[str],
) -> None:
    """Planning must not execute series-limit or other source queries."""
    _, query_context, datasource = _setup_chart_mocks(mocker)
    query_str = datasource.get_query_str_extended.return_value
    query_str.deferred = deferred
    query_str.prequeries = prequeries

    preparation = StreamingCSVExportCommand(query_context).prepare()

    assert preparation.ineligibility == StreamingExportIneligibility.QUERY_DEPENDENCY
    datasource.query.assert_not_called()
    datasource._raise_for_disallowed_sql.assert_not_called()


@pytest.mark.parametrize(
    ("attribute", "value"),
    [
        ("post_processing", [{"operation": "pivot"}]),
        ("time_offsets", ["1 year ago"]),
        ("annotation_layers", [{"annotationType": "FORMULA"}]),
        ("is_rowcount", True),
    ],
)
def test_prepare_rejects_python_result_transforms(
    mocker: MockerFixture,
    attribute: str,
    value: object,
) -> None:
    _, query_context, datasource = _setup_chart_mocks(mocker)
    setattr(query_context.queries[0], attribute, value)

    preparation = StreamingCSVExportCommand(query_context).prepare()

    assert not preparation.eligible
    assert preparation.ineligibility == StreamingExportIneligibility.RESULT_TRANSFORM
    datasource.get_query_str_extended.assert_not_called()


@pytest.mark.parametrize(
    ("attribute", "value"),
    [
        ("metrics", ["count"]),
        ("is_timeseries", True),
    ],
)
def test_prepare_rejects_dataframe_value_normalization(
    mocker: MockerFixture,
    attribute: str,
    value: object,
) -> None:
    _, query_context, datasource = _setup_chart_mocks(mocker)
    setattr(query_context.queries[0], attribute, value)

    preparation = StreamingCSVExportCommand(query_context).prepare()

    assert preparation.ineligibility == StreamingExportIneligibility.VALUE_NORMALIZATION
    datasource.get_query_str_extended.assert_not_called()


def test_prepare_rejects_datetime_normalization(mocker: MockerFixture) -> None:
    _, query_context, datasource = _setup_chart_mocks(mocker)
    datasource._collect_dttm_labels.return_value = (("event_time", "epoch_ms"),)

    preparation = StreamingCSVExportCommand(query_context).prepare()

    assert preparation.ineligibility == StreamingExportIneligibility.VALUE_NORMALIZATION
    datasource.get_query_str_extended.assert_not_called()


def test_prepare_rejects_multiple_queries(mocker: MockerFixture) -> None:
    _, query_context, datasource = _setup_chart_mocks(mocker)
    query_context.queries.append(mocker.MagicMock())

    preparation = StreamingCSVExportCommand(query_context).prepare()

    assert preparation.ineligibility == StreamingExportIneligibility.MULTIPLE_QUERIES
    datasource.get_query_str_extended.assert_not_called()


def test_prepare_rejects_dialect_without_server_side_cursor(
    mocker: MockerFixture,
) -> None:
    _, query_context, datasource = _setup_chart_mocks(mocker)
    engine = datasource.database.get_sqla_engine.return_value.__enter__.return_value
    engine.dialect.supports_server_side_cursors = False

    preparation = StreamingCSVExportCommand(query_context).prepare()

    assert preparation.ineligibility == StreamingExportIneligibility.UNSUPPORTED_DIALECT
    datasource.get_query_str_extended.assert_not_called()


def test_prepare_rejects_engine_without_normalization_contract(
    mocker: MockerFixture,
) -> None:
    _, query_context, datasource = _setup_chart_mocks(mocker)
    datasource.database.db_engine_spec.supports_direct_csv_streaming = False

    preparation = StreamingCSVExportCommand(query_context).prepare()

    assert preparation.ineligibility == StreamingExportIneligibility.UNSUPPORTED_ENGINE
    datasource.database.get_sqla_engine.assert_not_called()


def test_prepare_rejects_multi_statement_sql_after_mutation(
    mocker: MockerFixture,
) -> None:
    _, query_context, datasource = _setup_chart_mocks(mocker)
    datasource.database.mutate_sql_based_on_config.return_value = "SELECT 1; SELECT 2"
    datasource.database.mutate_sql_based_on_config.side_effect = None

    preparation = StreamingCSVExportCommand(query_context).prepare()

    assert preparation.ineligibility == StreamingExportIneligibility.MULTI_STATEMENT_SQL
    datasource._raise_for_disallowed_sql.assert_not_called()


def test_prepare_rejects_unsupported_csv_configuration(
    mocker: MockerFixture,
) -> None:
    _, query_context, datasource = _setup_chart_mocks(mocker)
    mocker.patch.dict(
        current_app.config,
        {"CSV_EXPORT": {"encoding": "utf-8", "compression": "gzip"}},
    )

    preparation = StreamingCSVExportCommand(query_context).prepare()

    assert preparation.ineligibility == StreamingExportIneligibility.CSV_CONFIGURATION
    datasource.get_query_str_extended.assert_not_called()


def test_csv_generation_with_small_dataset(mocker: MockerFixture) -> None:
    """Test CSV generation with a small dataset."""
    mock_db, query_context, datasource = _setup_chart_mocks(mocker)

    mock_result_proxy = mocker.MagicMock()
    mock_result_proxy.keys.return_value = ["col1", "col2", "col3"]
    mock_result_proxy.fetchmany.side_effect = [
        [
            ("row1_val1", "row1_val2", "row1_val3"),
            ("row2_val1", "row2_val2", "row2_val3"),
        ],
        [("row3_val1", "row3_val2", "row3_val3")],
        [],
    ]

    mock_connection = mocker.MagicMock()
    mock_connection.execution_options.return_value.execute.return_value = (
        mock_result_proxy
    )
    mock_connection.__enter__.return_value = mock_connection
    mock_connection.__exit__.return_value = None

    mock_engine = mocker.MagicMock()
    mock_engine.connect.return_value = mock_connection
    datasource.database.get_sqla_engine.return_value.__enter__.return_value = (
        mock_engine
    )

    command = StreamingCSVExportCommand(query_context, chunk_size=2)
    generator = command.run()

    chunks = list(generator)

    csv_data = b"".join(chunks).decode("utf-8-sig")
    lines = [line.strip() for line in csv_data.strip().split("\n")]

    assert len(lines) == 4
    assert lines[0] == "col1,col2,col3"
    assert "row1_val1,row1_val2,row1_val3" in csv_data
    assert "row2_val1,row2_val2,row2_val3" in csv_data
    assert "row3_val1,row3_val2,row3_val3" in csv_data


def test_csv_generation_with_special_characters(mocker: MockerFixture) -> None:
    """Test CSV generation properly escapes special characters."""
    mock_db, query_context, datasource = _setup_chart_mocks(mocker)

    mock_result = mocker.MagicMock()
    mock_result.keys.return_value = ["name", "description"]
    mock_result.fetchmany.side_effect = [
        [("John, Jr.", 'Quote"Test'), ("Line\nBreak", "Comma,Value")],
        [],
    ]

    mock_connection = mocker.MagicMock()
    mock_connection.execution_options.return_value.execute.return_value = mock_result
    mock_connection.__enter__.return_value = mock_connection
    mock_connection.__exit__.return_value = None

    mock_engine = mocker.MagicMock()
    mock_engine.connect.return_value = mock_connection
    datasource.database.get_sqla_engine.return_value.__enter__.return_value = (
        mock_engine
    )

    command = StreamingCSVExportCommand(query_context, chunk_size=10)
    generator = command.run()
    csv_data = b"".join(generator).decode("utf-8-sig")

    assert '"John, Jr."' in csv_data
    assert '"Quote""Test"' in csv_data
    assert "Line\nBreak" in csv_data
    assert '"Comma,Value"' in csv_data


def test_streaming_with_null_values(mocker: MockerFixture) -> None:
    """Test CSV generation handles NULL values correctly."""
    mock_db, query_context, datasource = _setup_chart_mocks(mocker)

    mock_result = mocker.MagicMock()
    mock_result.keys.return_value = ["col1", "col2", "col3"]
    mock_result.fetchmany.side_effect = [
        [("value1", None, "value3"), (None, "value2", None)],
        [],
    ]

    mock_connection = mocker.MagicMock()
    mock_connection.execution_options.return_value.execute.return_value = mock_result
    mock_connection.__enter__.return_value = mock_connection
    mock_connection.__exit__.return_value = None

    mock_engine = mocker.MagicMock()
    mock_engine.connect.return_value = mock_connection
    datasource.database.get_sqla_engine.return_value.__enter__.return_value = (
        mock_engine
    )

    command = StreamingCSVExportCommand(query_context, chunk_size=10)
    generator = command.run()
    csv_data = b"".join(generator).decode("utf-8-sig")

    lines = csv_data.strip().split("\n")
    assert len(lines) == 3
    assert "value1,,value3" in csv_data
    assert ",value2," in csv_data


def test_streaming_escapes_spreadsheet_formulas(mocker: MockerFixture) -> None:
    """Direct streaming applies the same CSV-injection defense as DataFrames."""
    _, query_context, datasource = _setup_chart_mocks(mocker)
    mock_result = mocker.MagicMock()
    mock_result.keys.return_value = ["=header", "value"]
    mock_result.fetchmany.side_effect = [
        [("=1+1", "+cmd"), ("-2", "normal")],
        [],
    ]
    connection = mocker.MagicMock()
    connection.execution_options.return_value.execute.return_value = mock_result
    connection.__enter__.return_value = connection
    engine = mocker.MagicMock()
    engine.dialect.supports_server_side_cursors = True
    engine.connect.return_value = connection
    datasource.database.get_sqla_engine.return_value.__enter__.return_value = engine

    csv_data = b"".join(StreamingCSVExportCommand(query_context).run()).decode(
        "utf-8-sig"
    )

    assert "'=header" in csv_data
    assert "'=1+1" in csv_data
    assert "'+cmd" in csv_data
    assert "-2,normal" in csv_data


def test_streaming_applies_engine_column_type_mutators(
    mocker: MockerFixture,
) -> None:
    """Direct rows preserve the normal datasource value-normalization contract."""

    _, query_context, datasource = _setup_chart_mocks(mocker)
    engine_spec = datasource.database.db_engine_spec
    engine_spec.requires_column_value_normalization = True
    engine_spec.normalize_column_values.side_effect = lambda values: [
        value * 1000 for value in values
    ]
    mock_result = mocker.MagicMock()
    mock_result.keys.return_value = ["duration"]
    mock_result.cursor.description = [("duration", 1186)]
    mock_result.fetchmany.side_effect = [[(2.5,)], []]
    connection = mocker.MagicMock()
    connection.execution_options.return_value.execute.return_value = mock_result
    connection.__enter__.return_value = connection
    engine = mocker.MagicMock()
    engine.dialect.supports_server_side_cursors = True
    engine.connect.return_value = connection
    datasource.database.get_sqla_engine.return_value.__enter__.return_value = engine

    csv_data = b"".join(StreamingCSVExportCommand(query_context).run()).decode(
        "utf-8-sig"
    )

    assert "2500.0" in csv_data
    engine_spec.normalize_column_values.assert_called_once_with([2.5])


def test_streaming_applies_supported_csv_export_configuration(
    mocker: MockerFixture,
) -> None:
    _, query_context, datasource = _setup_chart_mocks(mocker)
    mocker.patch.dict(
        current_app.config,
        {
            "CSV_EXPORT": {
                "date_format": "%Y/%m/%d",
                "encoding": "utf-8",
                "float_format": "%.1f",
                "lineterminator": "|\n",
                "na_rep": "NULL",
                "sep": ";",
            }
        },
    )
    mock_result = mocker.MagicMock()
    mock_result.keys.return_value = ["missing", "amount", "event_date"]
    mock_result.fetchmany.side_effect = [
        [(None, 1.25, date(2026, 7, 10))],
        [],
    ]
    connection = mocker.MagicMock()
    connection.execution_options.return_value.execute.return_value = mock_result
    connection.__enter__.return_value = connection
    engine = mocker.MagicMock()
    engine.dialect.supports_server_side_cursors = True
    engine.connect.return_value = connection
    datasource.database.get_sqla_engine.return_value.__enter__.return_value = engine

    csv_data = b"".join(StreamingCSVExportCommand(query_context).run()).decode(
        "utf-8-sig"
    )

    assert "missing;amount;event_date|\n" in csv_data
    assert "NULL;1.2;2026-07-10|\n" in csv_data


def test_direct_stream_error_is_not_written_into_csv(
    mocker: MockerFixture,
) -> None:
    _, query_context, datasource = _setup_chart_mocks(mocker)
    connection = mocker.MagicMock()
    connection.execution_options.return_value.execute.side_effect = RuntimeError(
        "database disconnected"
    )
    connection.__enter__.return_value = connection
    engine = mocker.MagicMock()
    engine.dialect.supports_server_side_cursors = True
    engine.connect.return_value = connection
    datasource.database.get_sqla_engine.return_value.__enter__.return_value = engine

    with pytest.raises(RuntimeError, match="database disconnected"):
        list(StreamingCSVExportCommand(query_context).run())


def test_streaming_execution_options_enabled(mocker: MockerFixture) -> None:
    """Test that streaming execution options are enabled."""
    mock_db, query_context, datasource = _setup_chart_mocks(mocker)

    mock_result_proxy = mocker.MagicMock()
    mock_result_proxy.keys.return_value = ["col1", "col2", "col3"]
    mock_result_proxy.fetchmany.side_effect = [
        [
            ("row1_val1", "row1_val2", "row1_val3"),
            ("row2_val1", "row2_val2", "row2_val3"),
        ],
        [("row3_val1", "row3_val2", "row3_val3")],
        [],
    ]

    mock_connection = mocker.MagicMock()
    mock_execution_options = mocker.MagicMock()
    mock_connection.execution_options.return_value = mock_execution_options
    mock_execution_options.execute.return_value = mock_result_proxy
    mock_connection.__enter__.return_value = mock_connection
    mock_connection.__exit__.return_value = None

    mock_engine = mocker.MagicMock()
    mock_engine.connect.return_value = mock_connection
    datasource.database.get_sqla_engine.return_value.__enter__.return_value = (
        mock_engine
    )

    command = StreamingCSVExportCommand(query_context)
    generator = command.run()
    list(generator)

    mock_connection.execution_options.assert_called_once_with(stream_results=True)


def test_empty_result_set(mocker: MockerFixture) -> None:
    """Test CSV generation with empty result set."""
    mock_db, query_context, datasource = _setup_chart_mocks(mocker)

    mock_result = mocker.MagicMock()
    mock_result.keys.return_value = ["col1", "col2"]
    mock_result.fetchmany.side_effect = [[]]

    mock_connection = mocker.MagicMock()
    mock_connection.execution_options.return_value.execute.return_value = mock_result
    mock_connection.__enter__.return_value = mock_connection
    mock_connection.__exit__.return_value = None

    mock_engine = mocker.MagicMock()
    mock_engine.connect.return_value = mock_connection
    datasource.database.get_sqla_engine.return_value.__enter__.return_value = (
        mock_engine
    )

    command = StreamingCSVExportCommand(query_context)
    generator = command.run()
    csv_data = b"".join(generator).decode("utf-8-sig")

    lines = [line.strip() for line in csv_data.strip().split("\n")]
    assert len(lines) == 1
    assert lines[0] == "col1,col2"


def test_catalog_and_schema_passed_to_engine(mocker: MockerFixture) -> None:
    """Test that catalog and schema are forwarded to get_sqla_engine.

    Prequeries (e.g. SET search_path for PostgreSQL) are now run automatically
    via a connect event listener registered inside get_sqla_engine, not by the
    streaming command itself.
    """
    mock_db, query_context, datasource = _setup_chart_mocks(
        mocker, catalog="my_catalog", schema="my_schema"
    )

    mock_result = mocker.MagicMock()
    mock_result.keys.return_value = ["col1"]
    mock_result.fetchmany.side_effect = [[("val",)], []]

    mock_connection = mocker.MagicMock()
    mock_connection.execution_options.return_value.execute.return_value = mock_result
    mock_connection.__enter__.return_value = mock_connection
    mock_connection.__exit__.return_value = None

    mock_engine = mocker.MagicMock()
    mock_engine.connect.return_value = mock_connection
    datasource.database.get_sqla_engine.return_value.__enter__.return_value = (
        mock_engine
    )

    command = StreamingCSVExportCommand(query_context)
    list(command.run())

    assert datasource.database.get_sqla_engine.call_count == 2
    assert datasource.database.get_sqla_engine.call_args_list == [
        mocker.call(catalog="my_catalog", schema="my_schema"),
        mocker.call(catalog="my_catalog", schema="my_schema"),
    ]
