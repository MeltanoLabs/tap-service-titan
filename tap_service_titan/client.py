"""REST client handling, including ServiceTitanStream base class."""

from __future__ import annotations

import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any

import requests
import requests.exceptions
from singer_sdk.pagination import BaseAPIPaginator, BasePageNumberPaginator
from singer_sdk.streams import RESTStream

from tap_service_titan.auth import ServiceTitanAuthenticator

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if sys.version_info >= (3, 13):
    from typing import TypeVar
else:
    from typing_extensions import TypeVar

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


DEFAULT_PAGE_SIZE = 5000


@dataclass
class DateRange:
    """Represents a date range for pagination."""

    start: datetime
    interval: timedelta
    max_date: datetime

    @property
    def end(self) -> datetime:
        """Calculate the end date of the current range."""
        return self.start + self.interval

    def increase(self) -> DateRange:
        """Create a new DateRange with the next interval."""
        return DateRange(self.end, self.interval, self.max_date)

    def is_valid(self) -> bool:
        """Check if the current range is within the maximum date."""
        return self.start < self.max_date


class DateRangePaginator(BaseAPIPaginator[DateRange]):
    """Paginator that uses date ranges for pagination."""

    @override
    def __init__(
        self,
        start_date: datetime,
        interval: timedelta,
        max_date: datetime,
    ) -> None:
        """Initialize DateRangePaginator.

        Args:
            start_date: Starting date for pagination
            interval: Time interval for each page
            max_date: Maximum date to paginate to
        """
        date_range = DateRange(start_date, interval, max_date)
        super().__init__(start_value=date_range)

    @override
    def get_next(self, response: requests.Response) -> DateRange | None:
        """Get the next date range for pagination.

        Args:
            response: The HTTP response (unused in this implementation)

        Returns:
            Next DateRange or None if no more pages
        """
        if not isinstance(self.current_value, DateRange):
            return None  # type: ignore[unreachable]

        new_range = self.current_value.increase()
        return new_range if new_range.is_valid() else None


_TToken = TypeVar("_TToken", default=int)


class ServiceTitanBaseStream(RESTStream[_TToken]):
    """ServiceTitan base stream class."""

    records_jsonpath = "$.data[*]"  # Or override `parse_response`.

    _LOG_REQUEST_METRIC_URLS: bool = (
        True  # Safe as params don't have sensitive info, but very helpful for debugging
    )

    _active_any: bool = False
    _sort_by: str | None = None
    _api_prefix: str = ""

    def __init_subclass__(
        cls,
        *,
        active_any: bool = False,
        sort_by: str | None = None,
        api_prefix: str | None = None,
    ) -> None:
        """Initialize the stream subclass.

        Args:
            active_any: Whether to include active and inactive records in the stream.
            sort_by: The field to sort the stream by.
            api_prefix: API path prefix inserted between the base URL and tenant ID,
                e.g. ``"/accounting/v2"``. When set, ``url_base`` becomes
                ``{api_url}{api_prefix}/tenant/{tenant_id}``.
        """
        cls._active_any = active_any
        cls._sort_by = sort_by
        if api_prefix is not None:
            cls._api_prefix = api_prefix
        return super().__init_subclass__()

    @property
    @override
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        base: str = self.config["api_url"]
        if self._api_prefix:
            return f"{base}{self._api_prefix}/tenant/{self.tenant_id}"
        return base

    @cached_property
    @override
    def authenticator(self) -> ServiceTitanAuthenticator:
        """Get an authenticator for Service Titan."""
        return ServiceTitanAuthenticator(
            client_id=self.config["client_id"],
            client_secret=self.config["client_secret"],
            auth_endpoint=self.config["auth_url"],
            oauth_scopes="",
        )

    @property
    @override
    def http_headers(self) -> dict[str, str]:
        """HTTP headers for each request."""
        headers = super().http_headers
        headers["ST-App-Key"] = self.config["st_app_key"]
        return headers

    @property
    def tenant_id(self) -> str:
        """The ServiceTitan tenant ID."""
        return self.config["tenant_id"]  # type: ignore[no-any-return]

    @override
    def backoff_max_tries(self) -> int:
        """The number of attempts before giving up when retrying requests.

        Had issues with 500's and 429's so we're going to try for a bit longer before
        bailing.

        Returns:
            Number of max retries.
        """
        return 10

    @override
    def response_error_message(self, response: requests.Response) -> str:
        """Build error message for invalid http statuses.

        WARNING - Override this method when the URL path may contain secrets or PII

        Args:
            response: A :class:`requests.Response` object.

        Returns:
            str: The error message
        """
        default = super().response_error_message(response)
        if response.content:
            try:
                json_response = response.json()
                if "title" in json_response:
                    title = json_response["title"]
                    return f"{default}. {title}"
            except (requests.exceptions.JSONDecodeError, ValueError):
                # Response content is not valid JSON - log the full content
                # This helps debug unexpected responses (e.g., HTML error pages)
                return f"{default}. Response body: {response.text}"
        return default


class ServiceTitanExportStream(ServiceTitanBaseStream):
    """ServiceTitan stream class for export endpoints."""

    next_page_token_jsonpath = "$.continueFrom"  # noqa: S105

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: str | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict[str, Any] = {}
        starting_date = self.get_starting_timestamp(context)

        if next_page_token:
            params["from"] = next_page_token

        # The Service Titan API uses the "from" param for both continuation tokens
        # and for the starting timestamp for the first request of an export
        if self.replication_key and starting_date and (next_page_token is None):
            # "from" param is inclusive of start date
            # this prevents duplicating of single record in each run
            starting_date += timedelta(milliseconds=1)
            params["from"] = starting_date.isoformat()

        if self._active_any:
            params["active"] = "Any"

        return params


class ServiceTitanPaginator(BasePageNumberPaginator):
    """ServiceTitan paginator class."""

    @override
    def has_more(self, response: requests.Response) -> bool:
        """Return True if there are more pages available."""
        return response.json().get("hasMore", False)  # type: ignore[no-any-return]


class ServiceTitanStream(ServiceTitanBaseStream[_TToken]):
    """ServiceTitan stream class for endpoints without export support."""

    _page_size: int
    _include_total: bool | None
    _first_page: int

    def __init_subclass__(
        cls,
        *,
        page_size: int = DEFAULT_PAGE_SIZE,
        include_total: bool | None = None,
        first_page: int = 1,
        **kwargs: Any,
    ) -> None:
        """Initialize the stream subclass.

        Args:
            page_size: Number of items to include in each page. Different endpoints support
                different limits. Defaults to 5000.
            include_total: Value to pass as URL parameter `includeTotal`. Defauls to omitting the
                parameter.
            first_page: First page number in paginated endpoints. Defaults to 1.
            **kwargs: Parameters for ``ServiceTitanBaseStream``.
        """
        cls._page_size = page_size
        cls._include_total = include_total
        cls._first_page = first_page
        super().__init_subclass__(**kwargs)

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: _TToken | None,
    ) -> dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict[str, Any] = {}
        if self.replication_key and (starting_date := self.get_starting_timestamp(context)):
            # Some endpoints use the "modifiedOnOrAfter" param for incremental extraction.
            # This is usually paired with a `modifiedOn` field in the response.
            starting_date += timedelta(milliseconds=1)
            params["modifiedOnOrAfter"] = starting_date.isoformat()

        if self._active_any:
            params["active"] = "Any"

        if self._sort_by:
            params["sort"] = f"+{self._sort_by}"

        if self._include_total is not None:
            params["includeTotal"] = self._include_total

        params["pageSize"] = self._page_size
        params["page"] = next_page_token

        return params

    @cached_property
    @override
    def is_sorted(self) -> bool:
        """Check if the stream is sorted."""
        return self._sort_by is not None

    @override
    def get_new_paginator(self) -> BaseAPIPaginator[Any] | None:
        """Create a new pagination helper instance."""
        return ServiceTitanPaginator(start_value=self._first_page)
