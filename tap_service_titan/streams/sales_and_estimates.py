"""Sales and estimates streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
from functools import cached_property

from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
from tap_service_titan.openapi_specs import SALESTECH, ServiceTitanSchema

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class EstimatesStream(ServiceTitanExportStream):
    """Define estimates stream."""

    name = "estimates"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(SALESTECH, key="Estimates.V2.ExportEstimatesResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/sales/v2/tenant/{self.tenant_id}/estimates/export"


class EstimateItemsStream(ServiceTitanStream, active_any=True):
    """Define estimate items stream.

    https://developer.servicetitan.io/api-details/#api=tenant-salestech-v2&operation=Estimates_GetItems
    """

    name = "estimate_items"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(SALESTECH, key="Estimates.V2.EstimateItemResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/sales/v2/tenant/{self.tenant_id}/estimates/items"
