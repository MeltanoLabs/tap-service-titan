"""Custom report streams for the ServiceTitan tap."""

from __future__ import annotations

import math
import sys
from dataclasses import KW_ONLY, dataclass
from datetime import datetime, timedelta, timezone
from functools import cached_property
from http import HTTPStatus
from typing import TYPE_CHECKING, Any

import requests
import requests.exceptions
from singer_sdk import Tap
from singer_sdk import typing as th
from singer_sdk.exceptions import RetriableAPIError

from tap_service_titan._common import now
from tap_service_titan.client import ServiceTitanStream

if sys.version_info >= (3, 11):
    from http import HTTPMethod
    from typing import NotRequired
else:
    from backports.httpmethod import HTTPMethod
    from typing_extensions import NotRequired

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if sys.version_info >= (3, 15):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict

if TYPE_CHECKING:
    from collections.abc import Generator, Iterable, Sequence
    from datetime import date

    from singer_sdk.helpers.types import Context, Record


class _ParameterDict(TypedDict, closed=True):  # type: ignore[call-arg]
    name: str
    value: str


class _Payload(TypedDict, closed=True):  # type: ignore[call-arg]
    parameters: list[_ParameterDict]


class _ReportConfig(TypedDict, closed=True):  # type: ignore[call-arg]
    report_name: str
    report_id: str
    report_category: str
    lookback_window_days: int
    parameters: list[_ParameterDict]

    backfill_date_parameter: NotRequired[str]


@dataclass
class _Parameter:
    _: KW_ONLY

    name: str
    value: str

    def to_dict(self) -> _ParameterDict:
        return {
            "name": self.name,
            "value": self.value,
        }


@dataclass
class _Report:
    _: KW_ONLY

    name: str
    category: str
    id: str
    lookback_window_days: int
    parameters: list[_Parameter]

    backfill: _Parameter | None = None

    @classmethod
    def from_dict(cls, report_dict: _ReportConfig) -> _Report:
        backfill: _Parameter | None = None
        backfill_date_parameter = report_dict.get("backfill_date_parameter")

        parameters = []
        for p in report_dict["parameters"]:
            param = _Parameter(name=p["name"], value=p["value"])
            parameters.append(param)

            # If the report config names a date parameter for day-by-day backfill,
            # use its configured start value to switch to incremental replication
            if backfill_date_parameter is not None and param.name == backfill_date_parameter:
                backfill = param

        return cls(
            name=report_dict["report_name"],
            category=report_dict["report_category"],
            id=report_dict["report_id"],
            lookback_window_days=report_dict["lookback_window_days"],
            parameters=parameters,
            backfill=backfill,
        )


class CustomReports(ServiceTitanStream, api_prefix="/reporting/v2"):
    """Define reviews stream."""

    http_method = HTTPMethod.POST
    is_sorted = True

    extra_retry_statuses: Sequence[int] = [
        HTTPStatus.CONFLICT,
        HTTPStatus.TOO_MANY_REQUESTS,
    ]

    @override
    def __init__(
        self,
        tap: Tap,
        report: _Report,
    ) -> None:
        """Initialize the stream."""
        self._report = report
        super().__init__(
            tap=tap,
            name=f"custom_report_{self._report.name}",
            path=f"/report-category/{self._report.category}/reports/{self._report.id}/data",
        )

        if report.backfill is not None:
            self._replication_key = report.backfill.name

        # Initialized lazily in get_records, after stream_state is available.
        self._curr_backfill_date: date | None = None

    @classmethod
    def from_report_dict(cls, *, tap: Tap, report: _ReportConfig) -> CustomReports:
        """Build a report stream from a dictionary."""
        return cls(tap=tap, report=_Report.from_dict(report))

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
            bookmark_date -= timedelta(days=self._report.lookback_window_days)
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
        self.requests_session.auth = self.authenticator
        resp = self.requests_session.get(
            f"{self.url_base}/report-category/{self._report.category}/reports/{self._report.id}",
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
        msg = f"Available parameters for custom report `{self._report.name}`: {metadata['parameters']}"  # noqa: E501
        self.logger.info(msg)
        properties: list[th.Property[Any]] = [
            th.Property(field["name"], self._get_datatype(field["dataType"]))
            for field in metadata["fields"]
        ]
        if self._report.backfill is not None:
            properties.append(
                th.Property(
                    self._report.backfill.name,
                    th.DateTimeType(),
                )
            )
        return th.PropertiesList(*properties).to_dict()

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
    ) -> _Payload:
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
        params_mapping = {p.name: p.to_dict() for p in self._report.parameters}
        if self._report.backfill is not None and self._curr_backfill_date:
            name = self._report.backfill.name
            params_mapping[name]["value"] = self._curr_backfill_date.strftime("%Y-%m-%d")

        params = list(params_mapping.values())
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
            if self._report.backfill is not None and self._curr_backfill_date is not None:
                data[self._report.backfill.name] = (
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
        if not self._report.backfill:
            yield from super().get_records(context)
            return

        # stream_state is available here (not in __init__), so initialize lazily.
        if self._curr_backfill_date is None:
            self._curr_backfill_date = self._get_initial_backfill_date(self._report.backfill.value)

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
