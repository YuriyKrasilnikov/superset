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
from __future__ import annotations

import contextlib
import logging
from datetime import datetime
from typing import Any, Callable, TYPE_CHECKING

from flask import (
    current_app as app,
    g,
    make_response,
    request,
    Response,
    stream_with_context,
)
from flask_appbuilder.api import expose, protect
from flask_babel import gettext as _
from kombu.exceptions import OperationalError
from marshmallow import ValidationError
from superset_core.tasks.types import TaskOptions
from werkzeug.utils import secure_filename

from superset import is_feature_enabled, security_manager
from superset.async_events.async_query_manager import AsyncQueryTokenException
from superset.charts.api import ChartRestApi
from superset.charts.client_processing import apply_client_processing
from superset.charts.data.dashboard_filter_context import (
    apply_dashboard_filter_context,
    DashboardFilterContext,
    get_dashboard_filter_context,
)
from superset.charts.data.query_context_cache_loader import QueryContextCacheLoader
from superset.charts.data.timing import (
    chart_data_request_timing,
    chart_timing_phase,
)
from superset.charts.schemas import ChartDataQueryContextSchema
from superset.commands.chart.data.create_async_job_command import (
    CreateAsyncChartDataJobCommand,
)
from superset.commands.chart.data.export_planner import (
    ChartDataExportMode,
    ChartDataExportPlan,
    ChartDataExportPlanner,
)
from superset.commands.chart.data.get_data_command import (
    ChartDataCommand,
    ChartDataExecutionOptions,
)
from superset.commands.chart.data.streaming_export_command import (
    StreamingCSVExportCommand,
)
from superset.commands.chart.exceptions import (
    ChartDataCacheLoadError,
    ChartDataQueryFailedError,
)
from superset.common.chart_data import ChartDataResultFormat, ChartDataResultType
from superset.common.chart_data_timing import (
    ChartDataExecutionResult,
    project_query_timing,
)
from superset.connectors.sqla.models import BaseDatasource
from superset.constants import CACHE_DISABLED_TIMEOUT
from superset.daos.exceptions import DatasourceNotFound
from superset.exceptions import (
    QueryObjectValidationError,
    SupersetException,
    SupersetSecurityException,
)
from superset.extensions import event_logger
from superset.models.sql_lab import Query
from superset.tasks.chart_data_exports import (
    generate_chart_data_export_artifact,
    serialize_query_context_for_artifact,
)
from superset.utils import json
from superset.utils.core import (
    create_zip,
    DatasourceType,
    get_user_id,
)
from superset.utils.decorators import logs_context
from superset.views.base import CsvResponse, generate_download_headers, XlsxResponse
from superset.views.base_api import statsd_metrics

if TYPE_CHECKING:
    from superset.common.query_context import QueryContext

logger = logging.getLogger(__name__)


class ChartDataRestApi(ChartRestApi):
    include_route_methods = {"get_data", "data", "data_from_cache"}

    @expose("/<int:pk>/data/", methods=("GET",))
    @protect()
    @statsd_metrics
    @chart_data_request_timing
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}.data",
        log_to_statsd=False,
        allow_extra_payload=True,
    )
    def get_data(  # noqa: C901
        self,
        pk: int,
        add_extra_log_payload: Callable[..., None] = lambda **kwargs: None,
    ) -> Response:
        """
        Take a chart ID and uses the query context stored when the chart was saved
        to return payload data response.
        ---
        get:
          summary: Return payload data response for a chart
          description: >-
            Takes a chart ID and uses the query context stored when the chart was saved
            to return payload data response. When filters_dashboard_id is provided,
            the chart's compiled SQL includes in scope dashboard filter
            default values.
          parameters:
          - in: path
            schema:
              type: integer
            name: pk
            description: The chart ID
          - in: query
            name: format
            description: The format in which the data should be returned
            schema:
              type: string
          - in: query
            name: type
            description: The type in which the data should be returned
            schema:
              type: string
          - in: query
            name: force
            description: Should the queries be forced to load from the source
            schema:
                type: boolean
          - in: query
            name: filters_dashboard_id
            description: >-
              Dashboard ID whose filter defaults should be applied to the
              chart's query context. The chart must belong to the specified dashboard.
              Only in scope filters with static default values are applied; filters that
              require a database query (I.E. defaultToFirstItem) or have no default are
              reported in the dashboard_filters response metadata.
            schema:
              type: integer
          responses:
            200:
              description: Query result
              content:
                application/json:
                  schema:
                    $ref: "#/components/schemas/ChartDataResponseSchema"
            202:
              description: Async job details
              content:
                application/json:
                  schema:
                    oneOf:
                    - $ref: "#/components/schemas/ChartDataAsyncResponseSchema"
                    - $ref: "#/components/schemas/ChartDataArtifactAsyncResponseSchema"
            400:
              $ref: '#/components/responses/400'
            401:
              $ref: '#/components/responses/401'
            403:
              $ref: '#/components/responses/403'
            404:
              $ref: '#/components/responses/404'
            500:
              $ref: '#/components/responses/500'
            503:
              $ref: '#/components/responses/503'
        """
        chart = self.datamodel.get(pk, self._base_filters)
        if not chart:
            return self.response_404()

        try:
            json_body = json.loads(chart.query_context)
        except (TypeError, json.JSONDecodeError):
            json_body = None

        if json_body is None:
            return self.response_400(
                message=_(
                    "Chart has no query context saved. Please save the chart again."
                )
            )

        # override saved query context
        json_body["result_format"] = request.args.get(
            "format", ChartDataResultFormat.JSON
        )
        json_body["result_type"] = request.args.get("type", ChartDataResultType.FULL)
        json_body["force"] = request.args.get("force")

        # Apply dashboard filter context when filters_dashboard_id is provided
        dashboard_filter_context: DashboardFilterContext | None = None
        if "filters_dashboard_id" in request.args:
            raw = request.args.get("filters_dashboard_id")
            try:
                filters_dashboard_id = int(raw)
            except (ValueError, TypeError):
                return self.response_400(
                    message="filters_dashboard_id must be an integer"
                )
        else:
            filters_dashboard_id = None

        if filters_dashboard_id is not None:
            try:
                dashboard_filter_context = get_dashboard_filter_context(
                    dashboard_id=filters_dashboard_id,
                    chart_id=pk,
                )
            except ValueError as error:
                return self.response_400(message=str(error))
            except SupersetSecurityException:
                return self.response_403()

            if efd := dashboard_filter_context.extra_form_data:
                # Note: this helper currently mutates `json_body` and `efd` in place.
                # Changes won't persist as these are dicts detached from the ORM state,
                # but highlighting in case they're further used (mind the changes).
                apply_dashboard_filter_context(json_body, efd)

        # We need to apply the form data to the global context as jinja
        # templating pulls form data from the request globally, so this
        # fallback ensures it has the filters and extra_form_data applied
        # when used in get_sqla_query which constructs the final query.
        g.form_data = json_body

        try:
            with chart_timing_phase("context"):
                query_context = self._create_query_context_from_form(json_body)
                command = ChartDataCommand(query_context)
            with chart_timing_phase("authorize"):
                command.validate()
        except DatasourceNotFound:
            return self.response_404()
        except SupersetSecurityException:
            return self.response_403()
        except QueryObjectValidationError as error:
            return self.response_400(message=error.message)
        except ValidationError as error:
            return self.response_400(
                message=_(
                    "Request is incorrect: %(error)s", error=error.normalized_messages()
                )
            )

        # TODO: support CSV, SQL query and other non-JSON types
        # Don't use async queries when cache is disabled (cache_timeout=-1)
        # as async queries depend on caching to retrieve results
        cache_timeout = query_context.get_cache_timeout()
        use_async = (
            is_feature_enabled("GLOBAL_ASYNC_QUERIES")
            and query_context.result_format == ChartDataResultFormat.JSON
            and query_context.result_type == ChartDataResultType.FULL
            and cache_timeout != CACHE_DISABLED_TIMEOUT
        )
        if use_async:
            return self._run_async(json_body, command, add_extra_log_payload)

        try:
            form_data = json.loads(chart.params)
        except (TypeError, json.JSONDecodeError):
            form_data = {}

        return self._get_data_response(
            command=command,
            form_data=form_data,
            datasource=query_context.datasource,
            add_extra_log_payload=add_extra_log_payload,
            dashboard_filter_context=dashboard_filter_context,
        )

    @expose("/data", methods=("POST",))
    @protect()
    @statsd_metrics
    @chart_data_request_timing
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: f"{self.__class__.__name__}.data",
        log_to_statsd=False,
        allow_extra_payload=True,
    )
    def data(  # noqa: C901
        self, add_extra_log_payload: Callable[..., None] = lambda **kwargs: None
    ) -> Response:
        """
        Take a query context constructed in the client and return payload
        data response for the given query
        ---
        post:
          summary: Return payload data response for the given query
          description: >-
            Takes a query context constructed in the client and returns payload data
            response for the given query.
          requestBody:
            description: >-
              A query context consists of a datasource from which to fetch data
              and one or many query objects.
            required: true
            content:
              application/json:
                schema:
                  $ref: "#/components/schemas/ChartDataQueryContextSchema"
          responses:
            200:
              description: Query result
              content:
                application/json:
                  schema:
                    $ref: "#/components/schemas/ChartDataResponseSchema"
            202:
              description: Async job details
              content:
                application/json:
                  schema:
                    oneOf:
                    - $ref: "#/components/schemas/ChartDataAsyncResponseSchema"
                    - $ref: "#/components/schemas/ChartDataArtifactAsyncResponseSchema"
            400:
              $ref: '#/components/responses/400'
            401:
              $ref: '#/components/responses/401'
            403:
              $ref: '#/components/responses/403'
            500:
              $ref: '#/components/responses/500'
            503:
              $ref: '#/components/responses/503'
        """
        json_body = None
        if request.is_json:
            json_body = request.json
        elif request.form.get("form_data"):
            # CSV export submits regular form data
            with contextlib.suppress(TypeError, json.JSONDecodeError):
                json_body = json.loads(request.form["form_data"])
        if json_body is None:
            return self.response_400(message=_("Request is not JSON"))

        try:
            with chart_timing_phase("context"):
                query_context = self._create_query_context_from_form(json_body)
                command = ChartDataCommand(query_context)
            with chart_timing_phase("authorize"):
                command.validate()
        except DatasourceNotFound:
            return self.response_404()
        except SupersetSecurityException:
            return self.response_403()
        except QueryObjectValidationError as error:
            return self.response_400(message=error.message)
        except ValidationError as error:
            return self.response_400(
                message=_(
                    "Request is incorrect: %(error)s", error=error.normalized_messages()
                )
            )

        # TODO: support CSV, SQL query and other non-JSON types
        # Don't use async queries when cache is disabled (cache_timeout=-1)
        # as async queries depend on caching to retrieve results
        cache_timeout = query_context.get_cache_timeout()
        use_async = (
            is_feature_enabled("GLOBAL_ASYNC_QUERIES")
            and query_context.result_format == ChartDataResultFormat.JSON
            and query_context.result_type == ChartDataResultType.FULL
            and cache_timeout != CACHE_DISABLED_TIMEOUT
        )
        if use_async:
            return self._run_async(json_body, command, add_extra_log_payload)

        form_data = json_body.get("form_data")
        filename, expected_rows = self._extract_export_params_from_request()

        return self._get_data_response(
            command,
            form_data=form_data,
            datasource=query_context.datasource,
            add_extra_log_payload=add_extra_log_payload,
            filename=filename,
            expected_rows=expected_rows,
        )

    @expose("/data/<cache_key>", methods=("GET",))
    @protect()
    @statsd_metrics
    @chart_data_request_timing
    @event_logger.log_this_with_context(
        action=lambda self, *args, **kwargs: (
            f"{self.__class__.__name__}.data_from_cache"
        ),
        log_to_statsd=False,
    )
    def data_from_cache(self, cache_key: str) -> Response:
        """
        Take a query context cache key and return payload
        data response for the given query.
        ---
        get:
          summary: Return payload data response for the given query
          description: >-
            Takes a query context cache key and returns payload data
            response for the given query.
          parameters:
          - in: path
            schema:
              type: string
            name: cache_key
          responses:
            200:
              description: Query result
              content:
                application/json:
                  schema:
                    $ref: "#/components/schemas/ChartDataResponseSchema"
            400:
              $ref: '#/components/responses/400'
            401:
              $ref: '#/components/responses/401'
            403:
              $ref: '#/components/responses/403'
            404:
              $ref: '#/components/responses/404'
            422:
              $ref: '#/components/responses/422'
            500:
              $ref: '#/components/responses/500'
        """
        try:
            with chart_timing_phase("context"):
                cached_data = self._load_query_context_form_from_cache(cache_key)
                # Set form_data in Flask Global as it is used as a fallback
                # for async queries with jinja context
                g.form_data = cached_data
                query_context = self._create_query_context_from_form(cached_data)
                command = ChartDataCommand(query_context)
            with chart_timing_phase("authorize"):
                command.validate()
        except ChartDataCacheLoadError:
            return self.response_404()
        except SupersetSecurityException:
            return self.response_403()
        except ValidationError as error:
            return self.response_400(
                message=_("Request is incorrect: %(error)s", error=error.messages)
            )

        return self._get_data_response(command, True)

    def _run_async(
        self,
        form_data: dict[str, Any],
        command: ChartDataCommand,
        add_extra_log_payload: Callable[..., None] | None = None,
    ) -> Response:
        """
        Execute command as an async query.
        """
        # First, look for the chart query results in the cache,
        # but only if we're not forcing a refresh.
        if not form_data.get("force"):
            with contextlib.suppress(ChartDataCacheLoadError):
                with chart_timing_phase("query"):
                    result = command.execute(
                        ChartDataExecutionOptions(force_cached=True)
                    )
                if result is not None:
                    # Log is_cached if extra payload callback is provided.
                    # This indicates no async job was triggered - data was already
                    # cached and a synchronous response is being returned immediately.
                    self._log_is_cached(result, add_extra_log_payload)
                    return self._send_chart_response(result)
        # Otherwise, kick off a background job to run the chart query.
        # Clients will either poll or be notified of query completion,
        # at which point they will call the /data/<cache_key> endpoint
        # to retrieve the results.
        with chart_timing_phase("enqueue"):
            async_command = CreateAsyncChartDataJobCommand()
            try:
                async_command.validate(request)
            except AsyncQueryTokenException:
                return self.response_401()

            async_result = async_command.run(form_data, get_user_id())
        return self.response(202, **async_result)

    def _send_chart_response(  # noqa: C901
        self,
        execution: ChartDataExecutionResult,
        form_data: dict[str, Any] | None = None,
        datasource: BaseDatasource | Query | None = None,
        filename: str | None = None,
        expected_rows: int | None = None,
        dashboard_filter_context: DashboardFilterContext | None = None,
    ) -> Response:
        result = execution.materialize()
        result_type = result["query_context"].result_type
        result_format = result["query_context"].result_format

        # Post-process the data so it matches the data presented in the chart.
        # This is needed for sending reports based on text charts that do the
        # post-processing of data, eg, the pivot table.
        if result_type == ChartDataResultType.POST_PROCESSED:
            with chart_timing_phase("client"):
                result = apply_client_processing(result, form_data, datasource)

        for query, query_result in zip(
            result["queries"], execution.queries, strict=True
        ):
            project_query_timing(query, query_result.timing)

        if result_format in ChartDataResultFormat.table_like():
            # Verify user has permission to export file
            if not self._can_export_data():
                return self.response_403()

            if not result["queries"]:
                return self.response_400(_("Empty query result"))

            is_csv_format = result_format == ChartDataResultFormat.CSV

            if len(result["queries"]) == 1:
                # return single query results
                data = result["queries"][0]["data"]
                if is_csv_format:
                    return CsvResponse(data, headers=generate_download_headers("csv"))

                return XlsxResponse(data, headers=generate_download_headers("xlsx"))

            # return multi-query results bundled as a zip file
            def _process_data(query_data: Any) -> Any:
                if result_format == ChartDataResultFormat.CSV:
                    # CSV data is already encoded to bytes by the query context
                    # processor, honoring the CSV_EXPORT encoding config.
                    if isinstance(query_data, str):
                        encoding = app.config["CSV_EXPORT"].get("encoding", "utf-8")
                        return query_data.encode(encoding)
                return query_data

            files = {
                f"query_{idx + 1}.{result_format}": _process_data(query["data"])
                for idx, query in enumerate(result["queries"])
            }
            return Response(
                create_zip(files),
                headers=generate_download_headers("zip"),
                mimetype="application/zip",
            )

        if result_format == ChartDataResultFormat.JSON:
            queries = result["queries"]
            if security_manager.is_guest_user():
                for query in queries:
                    query.pop("query", None)

            payload: dict[str, Any] = {"result": queries}
            if dashboard_filter_context is not None:
                payload["dashboard_filters"] = dashboard_filter_context.to_dict()

            with chart_timing_phase("serialize"):
                with event_logger.log_context(f"{self.__class__.__name__}.json_dumps"):
                    response_data = json.dumps(
                        payload,
                        default=json.json_int_dttm_ser,
                        ignore_nan=True,
                    )
            resp = make_response(response_data, 200)
            resp.headers["Content-Type"] = "application/json; charset=utf-8"
            return resp

        return self.response_400(message=f"Unsupported result_format: {result_format}")

    def _log_is_cached(
        self,
        result: ChartDataExecutionResult,
        add_extra_log_payload: Callable[..., None] | None,
    ) -> None:
        """
        Log is_cached values from query results to event logger.

        Extracts is_cached from each query in the result and logs it.
        If there's a single query, logs the boolean value directly.
        If multiple queries, logs as a list.
        """
        if add_extra_log_payload:
            is_cached_values = [
                query.payload.get("is_cached") for query in result.queries
            ]
            if len(is_cached_values) == 1:
                add_extra_log_payload(is_cached=is_cached_values[0])
            elif is_cached_values:
                add_extra_log_payload(is_cached=is_cached_values)

    @event_logger.log_this
    def _get_data_response(
        self,
        command: ChartDataCommand,
        force_cached: bool = False,
        form_data: dict[str, Any] | None = None,
        datasource: BaseDatasource | Query | None = None,
        filename: str | None = None,
        expected_rows: int | None = None,
        add_extra_log_payload: Callable[..., None] | None = None,
        dashboard_filter_context: DashboardFilterContext | None = None,
    ) -> Response:
        """Get data response and optionally log is_cached information."""
        query_context = command.query_context
        if (
            query_context.result_format in ChartDataResultFormat.table_like()
            and not self._can_export_data()
        ):
            return self.response_403()

        optimize_requested = self._should_attempt_direct_streaming(
            query_context,
            form_data,
            expected_rows,
        )
        async_preferred = self._async_response_preferred()
        artifact_available = (
            is_feature_enabled("CHART_DATA_ASYNC_EXPORTS") and get_user_id() is not None
        )
        try:
            plan = ChartDataExportPlanner(
                query_context,
                optimize_requested=optimize_requested,
                async_preferred=async_preferred,
                artifact_available=artifact_available,
                direct_command_factory=lambda: StreamingCSVExportCommand(
                    query_context,
                    chunk_size=1024,
                ),
            ).plan()
            if query_context.result_format == ChartDataResultFormat.CSV:
                self._record_export_plan(plan, async_preferred)
            if (
                plan.mode == ChartDataExportMode.DIRECT
                and plan.direct_command is not None
            ):
                return self._create_streaming_csv_response(
                    plan.direct_command,
                    form_data,
                    filename=filename,
                    expected_rows=expected_rows,
                )
            if plan.mode == ChartDataExportMode.ARTIFACT:
                return self._create_artifact_export_response(
                    query_context,
                    form_data,
                    filename,
                    preference_applied=plan.preference_applied,
                )
        except SupersetSecurityException:
            return self.response_403()
        except (QueryObjectValidationError, SupersetException) as exc:
            return self.response_400(message=str(exc))

        try:
            with chart_timing_phase("query"):
                result = command.execute(
                    ChartDataExecutionOptions(force_cached=force_cached)
                )
        except ChartDataCacheLoadError as exc:
            return self.response_422(message=exc.message)
        except ChartDataQueryFailedError as exc:
            return self.response_400(message=exc.message)

            # Log is_cached if extra payload callback is provided
        if add_extra_log_payload:
            is_cached_values = [
                query.payload.get("is_cached") for query in result.queries
            ]
            add_extra_log_payload(is_cached=is_cached_values)

        return self._send_chart_response(
            result,
            form_data,
            datasource,
            filename,
            expected_rows,
            dashboard_filter_context=dashboard_filter_context,
        )

    def _extract_export_params_from_request(self) -> tuple[str | None, int | None]:
        """Extract filename and expected_rows from request for streaming exports."""
        filename = request.form.get("filename")
        if filename:
            # Sanitize the user-supplied filename before it is used in the
            # Content-Disposition header (consistent with the generated-name
            # path). secure_filename may reduce a name consisting entirely of
            # unsupported characters to an empty string, in which case fall back
            # to the generated default downstream.
            filename = secure_filename(filename) or None
        if filename:
            logger.info("FRONTEND PROVIDED FILENAME: %s", filename)

        expected_rows = None
        if expected_rows_str := request.form.get("expected_rows"):
            try:
                expected_rows = int(expected_rows_str)
                logger.info("FRONTEND PROVIDED EXPECTED ROWS: %d", expected_rows)
            except (ValueError, TypeError):
                logger.warning("Invalid expected_rows value: %s", expected_rows_str)

        return filename, expected_rows

    # pylint: disable=invalid-name
    def _load_query_context_form_from_cache(self, cache_key: str) -> dict[str, Any]:
        return QueryContextCacheLoader.load(cache_key)

    def _map_form_data_datasource_to_dataset_id(
        self, form_data: dict[str, Any]
    ) -> dict[str, Any]:
        return {
            "dashboard_id": form_data.get("form_data", {}).get("dashboardId"),
            "dataset_id": (
                form_data.get("datasource", {}).get("id")
                if isinstance(form_data.get("datasource"), dict)
                and form_data.get("datasource", {}).get("type")
                == DatasourceType.TABLE.value
                else None
            ),
            "slice_id": form_data.get("form_data", {}).get("slice_id"),
        }

    @logs_context(context_func=_map_form_data_datasource_to_dataset_id)
    def _create_query_context_from_form(
        self, form_data: dict[str, Any]
    ) -> QueryContext:
        """
        Create the query context from the form data.

        :param form_data: The chart form data
        :returns: The query context
        :raises ValidationError: If the request is incorrect
        """

        try:
            return ChartDataQueryContextSchema().load(form_data)
        except KeyError as ex:
            raise ValidationError("Request is incorrect") from ex

    @staticmethod
    def _can_export_data() -> bool:
        if is_feature_enabled("GRANULAR_EXPORT_CONTROLS"):
            return security_manager.can_access("can_export_data", "Superset")
        return security_manager.can_access("can_csv", "Superset")

    def _should_attempt_direct_streaming(
        self,
        query_context: QueryContext,
        form_data: dict[str, Any] | None = None,
        expected_rows: int | None = None,
    ) -> bool:
        """Use row estimates only to select the pre-execution planning path."""
        if query_context.result_format != ChartDataResultFormat.CSV:
            return False

        threshold = app.config.get("CSV_STREAMING_ROW_THRESHOLD", 100000)
        row_estimate = expected_rows
        if row_estimate is None:
            candidate_form_data = form_data or query_context.form_data or {}
            row_limit = candidate_form_data.get("row_limit")
            try:
                row_estimate = int(row_limit) if row_limit is not None else None
            except (TypeError, ValueError):
                row_estimate = None
        return row_estimate is not None and row_estimate >= threshold

    @staticmethod
    def _record_export_plan(
        plan: ChartDataExportPlan,
        async_preferred: bool,
    ) -> None:
        """Emit bounded transport diagnostics without query or user content."""
        stats_logger = app.config["STATS_LOGGER"]
        stats_logger.incr(f"chart_data.export.transport.{plan.mode.value}")
        ineligibility = (
            plan.direct_ineligibility.value
            if plan.direct_ineligibility is not None
            else None
        )
        logger.info(
            "Chart CSV export plan selected: mode=%s, "
            "direct_ineligibility=%s, async_preferred=%s",
            plan.mode.value,
            ineligibility,
            async_preferred,
        )
        if ineligibility is not None:
            stats_logger.incr(f"chart_data.export.direct_ineligible.{ineligibility}")

    @staticmethod
    def _async_response_preferred() -> bool:
        """Return whether the request contains the RFC 7240 respond-async token."""
        for preference in request.headers.get("Prefer", "").split(","):
            if preference.strip().split(";", 1)[0].lower() == "respond-async":
                return True
        return False

    def _create_streaming_csv_response(
        self,
        command: StreamingCSVExportCommand,
        form_data: dict[str, Any] | None = None,
        filename: str | None = None,
        expected_rows: int | None = None,
    ) -> Response:
        """Create a streaming CSV response for large datasets."""
        # Use filename from frontend if provided, otherwise generate one
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            chart_name = "export"

            if form_data and form_data.get("slice_name"):
                chart_name = form_data["slice_name"]
            elif form_data and form_data.get("viz_type"):
                chart_name = form_data["viz_type"]

            # Sanitize chart name for filename
            filename = secure_filename(f"superset_{chart_name}_{timestamp}.csv")
        else:
            # Sanitize the client-provided filename before placing it in the
            # Content-Disposition header to avoid header/path injection.
            filename = secure_filename(filename) or "export.csv"

        logger.info("Creating streaming CSV response: %s", filename)
        if expected_rows:
            logger.info("Using expected_rows from frontend: %d", expected_rows)

        command.validate()

        csv_generator = command.run()

        # Get encoding from config
        encoding = app.config.get("CSV_EXPORT", {}).get("encoding", "utf-8")

        response = Response(
            stream_with_context(csv_generator),
            # Use content_type (not mimetype) so the charset is set verbatim;
            # passing a charset via mimetype makes Werkzeug append a second
            # charset, producing a malformed doubled Content-Type header.
            content_type=f"text/csv; charset={encoding}",
            headers={
                "Content-Disposition": f'attachment; filename="{filename}"',
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",  # Disable nginx buffering
            },
            direct_passthrough=False,  # Flask must iterate generator
        )

        # Force chunked transfer encoding
        response.implicit_sequence_conversion = False

        return response

    def _create_artifact_export_response(
        self,
        query_context: QueryContext,
        form_data: dict[str, Any] | None,
        filename: str | None,
        *,
        preference_applied: bool = False,
    ) -> Response:
        """Schedule a private GTF artifact for a non-direct large export."""
        owner_id = get_user_id()
        if owner_id is None:
            return self.response_403()
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            chart_name = (form_data or {}).get("slice_name") or (form_data or {}).get(
                "viz_type", "export"
            )
            filename = f"superset_{chart_name}_{timestamp}.csv"
        filename = secure_filename(filename) or "export.csv"
        try:
            task = generate_chart_data_export_artifact.schedule(
                serialize_query_context_for_artifact(query_context),
                owner_id,
                filename,
                options=TaskOptions(
                    task_name=f"Export {filename}"[:256],
                    timeout=app.config["CHART_DATA_ARTIFACT_TTL_SECONDS"],
                ),
            )
        except OperationalError:
            logger.exception("Chart-data export could not be enqueued")
            return self.response(
                503,
                message=_("The export service is temporarily unavailable"),
            )
        task_uuid = str(task.uuid)
        response = self.response(
            202,
            task_uuid=task_uuid,
            status="pending",
            status_url=f"/api/v1/task/{task_uuid}/status",
            artifact_url=f"/api/v1/task/{task_uuid}/artifact",
        )
        if preference_applied:
            response.headers["Preference-Applied"] = "respond-async"
        return response
