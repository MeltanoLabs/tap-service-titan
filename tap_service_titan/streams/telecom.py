"""Telecom streams for the ServiceTitan tap."""

from __future__ import annotations

from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
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


class OptOutsStream(ServiceTitanStream, api_prefix="/telecom/v3"):
    """Define opt-outs stream."""

    name = "opt_outs"
    path = "/optinouts/optouts"
    primary_keys = ("contactNumber",)
    schema = ServiceTitanSchema(TELECOM, key="Telecom.V3.OptOutResponse")
