"""Sales and estimates streams for the ServiceTitan tap."""

from __future__ import annotations

from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
from tap_service_titan.openapi_specs import SALESTECH, ServiceTitanSchema


class _BaseSalesExportStream(ServiceTitanExportStream, api_prefix="/sales/v2"):
    pass


class _BaseSalesStream(ServiceTitanStream, api_prefix="/sales/v2"):
    pass


class EstimatesStream(_BaseSalesExportStream):
    """Define estimates stream."""

    name = "estimates"
    path = "/estimates/export"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(SALESTECH, key="Estimates.V2.ExportEstimatesResponse")


class EstimateItemsStream(_BaseSalesStream, active_any=True):
    """Define estimate items stream.

    https://developer.servicetitan.io/api-details/#api=tenant-salestech-v2&operation=Estimates_GetItems
    """

    name = "estimate_items"
    path = "/estimates/items"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(SALESTECH, key="Estimates.V2.EstimateItemResponse")
