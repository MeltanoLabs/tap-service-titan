"""Custom report streams for the ServiceTitan tap."""

from __future__ import annotations

import math
import sys
from datetime import datetime, timedelta, timezone
from functools import cached_property
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import requests
import requests.exceptions
from singer_sdk import typing as th
from singer_sdk.exceptions import RetriableAPIError
from singer_sdk.singerlib.catalog import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL

from tap_service_titan._common import now
from tap_service_titan.client import ServiceTitanStream

if sys.version_info >= (3, 11):
    from http import HTTPMethod
else:
    from backports.httpmethod import HTTPMethod

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Mapping, Sequence
    from datetime import date

    from singer_sdk.helpers.types import Context, Record


class CustomReports(ServiceTitanStream):
    """Define reviews stream."""

    name = "custom_report"
    http_method = HTTPMethod.POST
    replication_method = REPLICATION_FULL_TABLE
    is_sorted = True

    extra_retry_statuses: Sequence[int] = [
        HTTPStatus.CONFLICT,
        HTTPStatus.TOO_MANY_REQUESTS,
    ]

    @override
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Initialize the stream."""
        self._report = kwargs.pop("report")
        kwargs["name"] = f"custom_report_{self._report['report_name']}"
        super().__init__(*args, **kwargs)

        # If the report config names a date parameter for day-by-day backfill,
        # find its configured start value and switch to incremental replication.
        backfill_param_name = self._report.get("backfill_date_parameter", "")
        self._backfill_start_value: str | None = next(
            (
                obj["value"]
                for obj in self._report["parameters"]
                if obj["name"] == backfill_param_name
            ),
            None,
        )
        if self._backfill_start_value:
            self.replication_method = REPLICATION_INCREMENTAL
            self.replication_key = backfill_param_name

        # Initialized lazily in get_records, after stream_state is available.
        self._curr_backfill_date: date | None = None

    # This data is sorted but we use a lookback window to get overlapping historical
    # data. This causes the sort check to fail because the bookmark gets updated to
    # an older value than previously saved.
    @override
    @property
    def check_sorted(self) -> bool:
        """Check if stream is sorted.

        This setting enables additional checks which may trigger
        `InvalidStreamSortException` if records are found which are unsorted.

        Returns:
            `True` if sorting is checked. Defaults to `True`.
        """
        return False

    def _get_initial_backfill_date(self, backfill_start_value: str) -> date:
        """Compute the starting date for the backfill loop.

        Uses the configured start date from the report parameters, but advances
        it to the bookmarked date (minus the lookback window) on subsequent runs.
        """
        configured = (
            datetime
            .strptime(backfill_start_value, "%Y-%m-%d")  # e.g 2026-02-10
            .replace(tzinfo=timezone.utc)
            .date()
        )
        bookmark = self.stream_state.get("replication_key_value")
        if bookmark:
            bookmark_date = datetime.strptime(bookmark, "%Y-%m-%dT%H:%M:%S%z").date()
            bookmark_date -= timedelta(days=self._report["lookback_window_days"])
            return max(configured, bookmark_date)
        return configured

    @staticmethod
    def _get_datatype(string_type: str) -> th.JSONTypeHelper[Any]:  # noqa: ARG004
        # TODO(maintainers): Use proper types once the API is fixed https://github.com/archdotdev/tap-service-titan/issues/67
        return th.StringType()
        # mapping = {
        #     # String , Number , Boolean , Date , Time
        #     "String": th.StringType(),
        #     "Number": th.NumberType(),
        #     "Boolean": th.BooleanType(),
        #     "Date": th.DateTimeType(),
        #     "Time": th.StringType(),
        # }
        # return mapping.get(string_type, th.StringType())

    def _get_report_metadata(self) -> dict[str, Any]:
        report_category = self._report["report_category"]
        report_id = self._report["report_id"]
        self.requests_session.auth = self.authenticator
        resp = self.requests_session.get(
            f"{self.url_base}/reporting/v2/tenant/{self.config['tenant_id']}/report-category/{report_category}/reports/{report_id}",
            headers=self.http_headers,
            timeout=self.timeout,
        )
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    @override
    @cached_property
    def schema(self) -> dict[str, Any]:
        """Get schema.

        Returns:
            JSON Schema dictionary for this stream.
        """
        metadata = self._get_report_metadata()
        msg = f"Available parameters for custom report `{self._report['report_name']}`: {metadata['parameters']}"  # noqa: E501
        self.logger.info(msg)
        properties: list[th.Property[Any]] = [
            th.Property(field["name"], self._get_datatype(field["dataType"]))
            for field in metadata["fields"]
        ]
        if self._report.get("backfill_date_parameter"):
            properties.append(
                th.Property(
                    self._report["backfill_date_parameter"],
                    th.DateTimeType(),
                )
            )
        return th.PropertiesList(*properties).to_dict()

    @override
    def get_url(self, context: Context | None) -> str:
        """Return the reporting URL."""
        report_category = self._report["report_category"]
        report_id = self._report["report_id"]
        return (
            f"{self.url_base}/reporting/v2/tenant/{self.tenant_id}"
            f"/report-category/{report_category}/reports/{report_id}/data"
        )

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params = super().get_url_params(context, next_page_token)
        params.pop("modifiedOnOrAfter", "")
        params["pageSize"] = 25000
        return params

    @override
    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> (
        Iterable[bytes]
        | str
        | bytes
        | list[tuple[Any, Any]]
        | tuple[tuple[Any, Any]]
        | Mapping[str, Any]
        | None
    ):
        """Prepare the data payload for the REST API request.

        By default, no payload will be sent (return None).

        Developers may override this method if the API requires a custom payload along
        with the request. (This is generally not required for APIs which use the
        HTTP 'GET' method.)

        Args:
            context: Stream partition or context dictionary.
            next_page_token: Token, page number or any request argument to request the
                next page of data.
        """
        params = self._report["parameters"]
        if self._curr_backfill_date:
            backfill_param_name = self._report["backfill_date_parameter"]
            params = [p for p in params if p["name"] != backfill_param_name]
            params.append({
                "name": backfill_param_name,
                "value": self._curr_backfill_date.strftime("%Y-%m-%d"),
            })
            self.logger.info("Custom report request parameters %s", params)
        return {"parameters": params}

    @override
    def parse_response(self, response: requests.Response) -> Iterable[Record]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        resp = response.json()
        field_names = [field["name"] for field in resp["fields"]]
        for record in resp["data"]:
            # TODO(maintainers): Use proper types once the API is fixed https://github.com/archdotdev/tap-service-titan/issues/67
            string_record = [str(val) if val is not None else "" for val in record]
            data = dict(zip(field_names, string_record, strict=False))
            # Add the backfill date to the record if configured
            if self._curr_backfill_date is not None:
                data[self._report["backfill_date_parameter"]] = (
                    self._curr_backfill_date.strftime("%Y-%m-%d") + "T00:00:00-00:00"
                )
            yield data

    @override
    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        Each record emitted should be a dictionary of property names to their values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per (possibly processed) record in the API.
        """
        if not self._backfill_start_value:
            yield from super().get_records(context)
            return

        # stream_state is available here (not in __init__), so initialize lazily.
        if self._curr_backfill_date is None:
            self._curr_backfill_date = self._get_initial_backfill_date(self._backfill_start_value)

        today = now().date()
        while self._curr_backfill_date <= today:
            yield from super().get_records(context)
            self._curr_backfill_date += timedelta(days=1)

    @override
    def backoff_wait_generator(self) -> Generator[float, None, None]:
        """Return a generator for backoff wait times."""

        def _backoff_from_headers(retriable_api_error: Exception) -> int:
            if (
                isinstance(
                    retriable_api_error,
                    (RetriableAPIError, requests.exceptions.HTTPError),
                )
                and retriable_api_error.response is not None
            ):
                response_headers = retriable_api_error.response.headers
                return math.ceil(float(response_headers.get("Retry-After", 0)))

            return 1

        return self.backoff_runtime(value=_backoff_from_headers)
