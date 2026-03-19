"""Inventory streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
from functools import cached_property
from typing import TYPE_CHECKING, Any

from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
from tap_service_titan.openapi_specs import INVENTORY, ServiceTitanSchema

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class PurchaseOrdersStream(ServiceTitanExportStream):
    """Define purchase orders stream."""

    name = "purchase_orders"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.ExportPurchaseOrdersResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/inventory/v2/tenant/{self.tenant_id}/export/purchase-orders"


class PurchaseOrderMarkupsStream(ServiceTitanStream):
    """Define purchase order markups stream.

    https://developer.servicetitan.io/api-details/#api=tenant-inventory-v2&operation=PurchaseOrdersMarkup_Get
    """

    name = "purchase_order_markups"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.Markups.PurchaseOrderMarkupResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/inventory/v2/tenant/{self.tenant_id}/purchase-order-markups"


class PurchaseOrderTypesStream(ServiceTitanStream):
    """Define purchase order types stream."""

    name = "purchase_order_types"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.PurchaseOrderTypeResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/inventory/v2/tenant/{self.tenant_id}/purchase-order-types"


class ReceiptsStream(ServiceTitanStream, active_any=True):
    """Define receipts stream."""

    name = "receipts"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.InventoryReceiptResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/inventory/v2/tenant/{self.tenant_id}/receipts"


class ReturnsStream(ServiceTitanStream, active_any=True):
    """Define returns stream."""

    name = "returns"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.InventoryReturnResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/inventory/v2/tenant/{self.tenant_id}/returns"


class AdjustmentsStream(ServiceTitanStream):
    """Define adjustments stream."""

    name = "adjustments"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.InventoryAdjustmentResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/inventory/v2/tenant/{self.tenant_id}/adjustments"


class ReturnTypesStream(ServiceTitanStream):
    """Define return types stream.

    https://developer.servicetitan.io/api-details/#api=tenant-inventory-v2&operation=ReturnTypes_GetList
    """

    name = "return_types"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.ListReturnTypesResponse")

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        # This endpoint has an undocumented max page size of 500
        params["activeOnly"] = False
        return params

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/inventory/v2/tenant/{self.tenant_id}/return-types"


class TransfersStream(ServiceTitanStream):
    """Define transfers stream."""

    name = "transfers"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.InventoryTransferResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/inventory/v2/tenant/{self.tenant_id}/transfers"


class TrucksStream(ServiceTitanStream):
    """Define trucks stream."""

    name = "trucks"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.TruckResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/inventory/v2/tenant/{self.tenant_id}/trucks"


class VendorsStream(ServiceTitanStream):
    """Define vendors stream."""

    name = "vendors"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.VendorResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/inventory/v2/tenant/{self.tenant_id}/vendors"


class WarehousesStream(ServiceTitanStream):
    """Define warehouses stream."""

    name = "warehouses"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.WarehouseResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/inventory/v2/tenant/{self.tenant_id}/warehouses"
