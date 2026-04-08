"""Inventory streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
from tap_service_titan.openapi_specs import INVENTORY, ServiceTitanSchema

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context


class _BaseInventoryStream(ServiceTitanStream, api_prefix="/inventory/v2"):
    pass


class _BaseInventoryExportStream(ServiceTitanExportStream, api_prefix="/inventory/v2"):
    pass


class PurchaseOrdersStream(_BaseInventoryExportStream):
    """Define purchase orders stream."""

    name = "purchase_orders"
    path = "/export/purchase-orders"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.ExportPurchaseOrdersResponse")


class PurchaseOrderMarkupsStream(_BaseInventoryStream):
    """Define purchase order markups stream.

    https://developer.servicetitan.io/api-details/#api=tenant-inventory-v2&operation=PurchaseOrdersMarkup_Get
    """

    name = "purchase_order_markups"
    path = "/purchase-order-markups"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.Markups.PurchaseOrderMarkupResponse")


class PurchaseOrderTypesStream(_BaseInventoryStream):
    """Define purchase order types stream."""

    name = "purchase_order_types"
    path = "/purchase-order-types"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.PurchaseOrderTypeResponse")


class ReceiptsStream(_BaseInventoryStream, active_any=True):
    """Define receipts stream."""

    name = "receipts"
    path = "/receipts"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.InventoryReceiptResponse")


class ReturnsStream(_BaseInventoryStream, active_any=True):
    """Define returns stream."""

    name = "returns"
    path = "/returns"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.InventoryReturnResponse")


class AdjustmentsStream(_BaseInventoryStream):
    """Define adjustments stream."""

    name = "adjustments"
    path = "/adjustments"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.InventoryAdjustmentResponse")


class ReturnTypesStream(_BaseInventoryStream):
    """Define return types stream.

    https://developer.servicetitan.io/api-details/#api=tenant-inventory-v2&operation=ReturnTypes_GetList
    """

    name = "return_types"
    path = "/return-types"
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


class TransfersStream(_BaseInventoryStream):
    """Define transfers stream."""

    name = "transfers"
    path = "/transfers"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.InventoryTransferResponse")


class TrucksStream(_BaseInventoryStream):
    """Define trucks stream."""

    name = "trucks"
    path = "/trucks"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.TruckResponse")


class VendorsStream(_BaseInventoryStream):
    """Define vendors stream."""

    name = "vendors"
    path = "/vendors"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.VendorResponse")


class WarehousesStream(_BaseInventoryStream):
    """Define warehouses stream."""

    name = "warehouses"
    path = "/warehouses"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(INVENTORY, key="Inventory.V2.WarehouseResponse")
