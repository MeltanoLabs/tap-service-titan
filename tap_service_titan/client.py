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
            return None

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

    def __init_subclass__(cls, *, active_any: bool = False, sort_by: str | None = None) -> None:
        """Initialize the stream subclass.

        Args:
            active_any: Whether to include active and inactive records in the stream.
            sort_by: The field to sort the stream by.
        """
        cls._active_any = active_any
        cls._sort_by = sort_by
        return super().__init_subclass__()

    @override
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config["api_url"]

    @override
    @cached_property
    def authenticator(self) -> ServiceTitanAuthenticator:
        """Get an authenticator for Service Titan."""
        return ServiceTitanAuthenticator(
            client_id=self.config["client_id"],
            client_secret=self.config["client_secret"],
            auth_endpoint=self.config["auth_url"],
            oauth_scopes="",
        )

    @override
    @property
    def http_headers(self) -> dict:
        """HTTP headers for each request."""
        headers = super().http_headers
        headers["ST-App-Key"] = self.config["st_app_key"]
        return headers

    @property
    def tenant_id(self) -> str:
        """The ServiceTitan tenant ID."""
        return self.config["tenant_id"]

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
        params: dict = {}
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
        return response.json().get("hasMore", False)


class ServiceTitanStream(ServiceTitanBaseStream[_TToken]):
    """ServiceTitan stream class for endpoints without export support."""

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
        params: dict = {}
        if self.replication_key and (starting_date := self.get_starting_timestamp(context)):
            # Some endpoints use the "modifiedOnOrAfter" param for incremental extraction.
            # This is usually paired with a `modifiedOn` field in the response.
            starting_date += timedelta(milliseconds=1)
            params["modifiedOnOrAfter"] = starting_date.isoformat()

        if self._active_any:
            params["active"] = "Any"

        if self._sort_by:
            params["sort"] = f"+{self._sort_by}"

        params["pageSize"] = 5000
        params["page"] = next_page_token

        return params

    @override
    @cached_property
    def is_sorted(self) -> bool:
        """Check if the stream is sorted."""
        return self._sort_by is not None

    @override
    def get_new_paginator(self) -> BaseAPIPaginator | None:
        """Create a new pagination helper instance."""
        return ServiceTitanPaginator(start_value=1)
