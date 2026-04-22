"""Telecom streams for the ServiceTitan tap."""

from __future__ import annotations

from tap_service_titan.client import ServiceTitanExportStream
from tap_service_titan.openapi_specs import TELECOM, ServiceTitanSchema


class _BaseTelecomExportStream(ServiceTitanExportStream, api_prefix="/telecom/v2"):
    pass


class CallsStream(_BaseTelecomExportStream):
    """Define calls stream."""

    name = "calls"
    path = "/export/calls"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(TELECOM, key="Telecom.V2.ExportCallResponse")
