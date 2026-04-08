"""Pricebook streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
from typing import Any

from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
from tap_service_titan.openapi_specs import PRICEBOOK, ServiceTitanSchema

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class _BasePricebookStream(ServiceTitanStream, api_prefix="/pricebook/v2"):
    pass


class _BasePricebookExportStream(ServiceTitanExportStream, api_prefix="/pricebook/v2"):
    pass


class ClientSpecificPricingStream(_BasePricebookStream, active_any=True):
    """Define client-specific pricing stream.

    https://developer.servicetitan.io/api-details/#api=tenant-pricebook-v2&operation=ClientSpecificPricing_GetAllRateSheets
    """

    name = "client_specific_pricing"
    path = "/clientspecificpricing"
    primary_keys = ("id",)
    schema = ServiceTitanSchema(
        PRICEBOOK,
        key="Estimates.V2.ClientSpecificPricingResponse",
    )


class PricebookCategoriesStream(_BasePricebookStream, active_any=True):
    """Define pricebook categories stream.

    https://developer.servicetitan.io/api-details/#api=tenant-pricebook-v2&operation=Categories_GetList
    """

    name = "categories"
    path = "/categories"
    primary_keys = ("id",)
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.CategoryResponse")


class DiscountsAndFeesStream(_BasePricebookStream, active_any=True, sort_by="ModifiedOn"):
    """Define discounts and fees stream.

    https://developer.servicetitan.io/api-details/#api=tenant-pricebook-v2&operation=DiscountAndFees_GetList
    """

    name = "discounts_and_fees"
    path = "/discounts-and-fees"
    primary_keys = ("id",)
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.DiscountAndFeesResponse")


class EquipmentStream(_BasePricebookStream, active_any=True, sort_by="ModifiedOn"):
    """Define equipment stream.

    https://developer.servicetitan.io/api-details/#api=tenant-pricebook-v2&operation=Equipment_GetList
    """

    name = "equipment"
    path = "/equipment"
    primary_keys = ("id",)
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.EquipmentResponse")


class MaterialsStream(_BasePricebookStream, active_any=True):
    """Define materials stream.

    https://developer.servicetitan.io/api-details/#api=tenant-pricebook-v2&operation=Materials_GetList
    """

    name = "materials"
    path = "/materials"
    primary_keys = ("id",)
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.MaterialResponse")


class MaterialsMarkupStream(_BasePricebookStream):
    """Define materials markup stream.

    https://developer.servicetitan.io/api-details/#api=tenant-pricebook-v2&operation=MaterialsMarkup_GetList
    """

    name = "materials_markup"
    path = "/materialsmarkup"
    primary_keys = ("id",)
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.MaterialsMarkupResponse")


class ServicesStream(_BasePricebookStream, active_any=True):
    """Define services stream.

    https://developer.servicetitan.io/api-details/#api=tenant-pricebook-v2&operation=Services_GetList
    """

    name = "services"
    path = "/services"
    primary_keys = ("id",)
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.ServiceGetResponse")

    @override
    def get_url_params(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        params = super().get_url_params(*args, **kwargs)

        # If true, the prices will be calculated based on the current dynamic pricing rules.
        params["calculatePrices"] = True
        return params


class ExportEquipmentStream(_BasePricebookExportStream):
    """Define export equipment stream."""

    name = "export_equipment"
    path = "/export/equipment"
    primary_keys = ("id",)
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.ExportEquipmentResponse")


class ExportMaterialsStream(_BasePricebookExportStream):
    """Define export materials stream."""

    name = "export_materials"
    path = "/export/materials"
    primary_keys = ("id",)
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.ExportMaterialResponse")


class ExportServicesStream(_BasePricebookExportStream):
    """Define export services stream."""

    name = "export_services"
    path = "/export/services"
    primary_keys = ("id",)
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(PRICEBOOK, key="Pricebook.V2.ExportServiceResponse")
