"""Sales and estimates streams for the ServiceTitan tap."""

from __future__ import annotations

from tap_service_titan.client import ServiceTitanExportStream
from tap_service_titan.openapi_specs import SALESTECH, ServiceTitanSchema


class _BaseSalesStream(ServiceTitanExportStream, api_prefix="/sales/v2"):
    pass


class EstimatesStream(_BaseSalesStream):
    """Define estimates stream."""

    name = "estimates"
    path = "/estimates/export"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(SALESTECH, key="Estimates.V2.ExportEstimatesResponse")
