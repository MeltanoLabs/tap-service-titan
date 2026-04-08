"""Job booking streams for the ServiceTitan tap."""

from __future__ import annotations

from tap_service_titan.client import ServiceTitanStream
from tap_service_titan.openapi_specs import JBCE, ServiceTitanSchema


class _BaseJobBookingStream(ServiceTitanStream, api_prefix="/jbce/v2"):
    pass


class CallReasonsStream(_BaseJobBookingStream):
    """Define call reasons stream."""

    name = "call_reasons"
    path = "/call-reasons"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JBCE, key="Jbce.V2.CallReasonResponse")
