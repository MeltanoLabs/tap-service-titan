"""Pricebook streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
from functools import cached_property
from typing import Any

from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
from tap_service_titan.openapi_specs import PRICEBOOK, ServiceTitanSchema

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class ClientSpecificPricingStream(ServiceTitanStream, active_any=True):
    """Define client-specific pricing stream.

    https://developer.servicetitan.io/api-details/#api=tenant-pricebook-v2&operation=ClientSpecificPricing_GetAllRateSheets
    """

    name = "client_specific_pricing"
    primary_keys = ("id",)
    schema = ServiceTitanSchema(
        PRICEBOOK,
        key="Estimates.V2.ClientSpecificPricingResponse",
    )

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/pricebook/v2/tenant/{self.tenant_id}/clientspecificpricing"


class PricebookCategoriesStream(ServiceTitanStream, active_any=True):
    """Define pricebook categories stream.

    https://developer.servicetitan.io/api-details/#api=tenant-pricebook-v2&operation=Categories_GetList
    """

    name = "categories"
    primary_keys = ("id",)
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.CategoryResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/pricebook/v2/tenant/{self.tenant_id}/categories"


class DiscountsAndFeesStream(ServiceTitanStream, active_any=True, sort_by="ModifiedOn"):
    """Define discounts and fees stream.

    https://developer.servicetitan.io/api-details/#api=tenant-pricebook-v2&operation=DiscountAndFees_GetList
    """

    name = "discounts_and_fees"
    primary_keys = ("id",)
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.DiscountAndFeesResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/pricebook/v2/tenant/{self.tenant_id}/discounts-and-fees"


class EquipmentStream(ServiceTitanStream, active_any=True, sort_by="ModifiedOn"):
    """Define equipment stream.

    https://developer.servicetitan.io/api-details/#api=tenant-pricebook-v2&operation=Equipment_GetList
    """

    name = "equipment"
    primary_keys = ("id",)
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.EquipmentResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/pricebook/v2/tenant/{self.tenant_id}/equipment"


class MaterialsStream(ServiceTitanStream, active_any=True):
    """Define materials stream.

    https://developer.servicetitan.io/api-details/#api=tenant-pricebook-v2&operation=Materials_GetList
    """

    name = "materials"
    primary_keys = ("id",)
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.MaterialResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/pricebook/v2/tenant/{self.tenant_id}/materials"


class MaterialsMarkupStream(ServiceTitanStream):
    """Define materials markup stream.

    https://developer.servicetitan.io/api-details/#api=tenant-pricebook-v2&operation=MaterialsMarkup_GetList
    """

    name = "materials_markup"
    primary_keys = ("id",)
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.MaterialsMarkupResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/pricebook/v2/tenant/{self.tenant_id}/materialsmarkup"


class ServicesStream(ServiceTitanStream, active_any=True):
    """Define services stream.

    https://developer.servicetitan.io/api-details/#api=tenant-pricebook-v2&operation=Services_GetList
    """

    name = "services"
    primary_keys = ("id",)
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.ServiceGetResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/pricebook/v2/tenant/{self.tenant_id}/services"

    @override
    def get_url_params(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        params = super().get_url_params(*args, **kwargs)

        # If true, the prices will be calculated based on the current dynamic pricing rules.
        params["calculatePrices"] = True
        return params


class ExportEquipmentStream(ServiceTitanExportStream):
    """Define export equipment stream."""

    name = "export_equipment"
    primary_keys = ("id",)
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.ExportEquipmentResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/pricebook/v2/tenant/{self.tenant_id}/export/equipment"


class ExportMaterialsStream(ServiceTitanExportStream):
    """Define export materials stream."""

    name = "export_materials"
    primary_keys = ("id",)
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.ExportMaterialResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/pricebook/v2/tenant/{self.tenant_id}/export/materials"


class ExportServicesStream(ServiceTitanExportStream):
    """Define export services stream."""

    name = "export_services"
    primary_keys = ("id",)
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.ExportServiceResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/pricebook/v2/tenant/{self.tenant_id}/export/services"
