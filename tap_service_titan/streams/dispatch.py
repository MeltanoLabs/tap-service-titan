"""Dispatch streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any

from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator

from tap_service_titan._common import now
from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
from tap_service_titan.openapi_specs import DISPATCH, ServiceTitanSchema

if sys.version_info >= (3, 11):
    from http import HTTPMethod
else:
    from backports.httpmethod import HTTPMethod

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    import requests
    from singer_sdk.helpers.types import Context


class CapacitiesPaginator(BaseAPIPaginator[datetime]):
    """Define paginator for the capacities stream."""

    @override
    def __init__(
        self,
        start_value: datetime,
        *,
        lookahead_days: int = 14,
        **kwargs: Any,
    ) -> None:
        """Initialize the paginator.

        Args:
            start_value: The starting date for pagination.
            lookahead_days: The number of days to look ahead.
            **kwargs: Additional keyword arguments.
        """
        super().__init__(start_value=start_value, **kwargs)

        self.lookahead_days = lookahead_days
        self.end_value = now() + timedelta(days=self.lookahead_days)

    @override
    def has_more(self, response: requests.Response) -> bool:
        """Check if there are more requests to make."""
        return self.current_value <= self.end_value

    @override
    def get_next(self, response: requests.Response) -> datetime | None:
        """Get the next page token."""
        return self.current_value + timedelta(days=1)


class CapacitiesStream(ServiceTitanStream[datetime]):
    """Define capacities stream."""

    name = "capacities"
    primary_keys = (
        "startUtc",
        "businessUnitIds",
    )
    replication_key = "startUtc"
    http_method = HTTPMethod.POST
    records_jsonpath = "$.availabilities[*]"
    schema = ServiceTitanSchema(
        DISPATCH,
        key="Dispatch.V2.CapacityResponseAvailability",
    )

    @override
    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        for availability_dict in extract_jsonpath(self.records_jsonpath, input=response.json()):
            if availability_dict.get("technicians", []):
                yield availability_dict

    @override
    def get_new_paginator(self) -> CapacitiesPaginator | None:
        """Get the paginator."""
        if start_date := self.get_starting_timestamp(self.context):
            start_date = min(start_date, now())
            # Set the time to the start of the day to capture late updates
            return CapacitiesPaginator(
                start_date.replace(
                    hour=0,
                    minute=0,
                    second=0,
                    microsecond=0,
                )
            )

        return None

    @override
    def prepare_request_payload(
        self,
        context: Context | None,
        next_page_token: datetime | None,
    ) -> (
        Iterable[bytes]
        | str
        | bytes
        | list[tuple[Any, Any]]
        | tuple[tuple[Any, Any]]
        | Mapping[str, Any]
        | None
    ):
        """Prepare the request payload."""
        payload = {"skillBasedAvailability": "false"}
        if next_page_token:
            payload["startsOnOrAfter"] = next_page_token.isoformat()
            payload["endsOnOrBefore"] = (next_page_token + timedelta(days=1)).isoformat()

        return payload

    @override
    def get_url_params(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        """Return an empty dictionary of URL parameters.

        This endpoint does not accept any URL parameters.
        """
        return {}

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/dispatch/v2/tenant/{self.tenant_id}/capacity"


class ArrivalWindowsStream(ServiceTitanStream, active_any=True):
    """Define arrival windows stream.

    https://developer.servicetitan.io/api-details/#api=tenant-dispatch-v2&operation=ArrivalWindows_GetList
    """

    name = "arrival_windows"
    primary_keys = ("id",)
    schema = ServiceTitanSchema(DISPATCH, key="Dispatch.V2.ArrivalWindowResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/dispatch/v2/tenant/{self.tenant_id}/arrival-windows"


class AppointmentAssignmentsStream(ServiceTitanExportStream, active_any=True):
    """Define appointment assignments stream.

    https://developer.servicetitan.io/api-details/#api=tenant-dispatch-v2&operation=Export_AppointmentAssignments
    """

    name = "appointment_assignments"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        DISPATCH,
        key="Dispatch.V2.ExportAppointmentAssignmentsResponse",
    )

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/dispatch/v2/tenant/{self.tenant_id}/export/appointment-assignments"


class NonJobAppointmentsStream(ServiceTitanStream):
    """Define non job appointments stream."""

    name = "non_job_appointments"
    primary_keys = ("id",)
    replication_key: str = "createdOn"
    schema = ServiceTitanSchema(DISPATCH, key="Dispatch.V2.NonJobAppointmentResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/dispatch/v2/tenant/{self.tenant_id}/non-job-appointments"


class TeamsStream(ServiceTitanStream):
    """Define teams stream."""

    name = "teams"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(DISPATCH, key="Dispatch.V2.TeamResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/dispatch/v2/tenant/{self.tenant_id}/teams"


class TechnicianShiftsStream(ServiceTitanStream, active_any=True):
    """Define technician shifts stream.

    https://developer.servicetitan.io/api-details/#api=tenant-dispatch-v2&operation=TechnicianShifts_GetList
    """

    name = "technician_shifts"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(DISPATCH, key="Dispatch.V2.TechnicianShiftResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/dispatch/v2/tenant/{self.tenant_id}/technician-shifts"


class ZonesStream(ServiceTitanStream):
    """Define zones stream."""

    name = "zones"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(DISPATCH, key="Dispatch.V2.ZoneResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/dispatch/v2/tenant/{self.tenant_id}/zones"


class BusinessHoursStream(ServiceTitanStream):
    """Define business hours stream."""

    name = "business_hours"
    primary_keys = ()
    schema = ServiceTitanSchema(DISPATCH, key="Dispatch.V2.BusinessHourModel")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/dispatch/v2/tenant/{self.tenant_id}/business-hours"


class TechnicianSkillsStream(ServiceTitanStream, active_any=True):
    """Define technician skills stream.

    https://developer.servicetitan.io/api-details/#api=tenant-dispatch-v2&operation=TechnicianSkills_GetList
    """

    name = "technician_skills"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(DISPATCH, key="Dispatch.V2.TechnicianSkillResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/dispatch/v2/tenant/{self.tenant_id}/technician-skills"
