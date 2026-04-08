"""Marketing streams for the ServiceTitan tap."""

from __future__ import annotations

from tap_service_titan.client import ServiceTitanStream
from tap_service_titan.openapi_specs import MARKETING, ServiceTitanSchema


class _BaseMarketingStream(ServiceTitanStream, api_prefix="/marketing/v2"):
    pass


class CampaignsStream(_BaseMarketingStream, active_any=True):
    """Define campaigns stream.

    https://developer.servicetitan.io/api-details/#api=tenant-marketing-v2&operation=Campaigns_GetList
    """

    name = "campaigns"
    path = "/campaigns"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(MARKETING, key="Marketing.V2.CampaignResponse")


class MarketingCategoriesStream(_BaseMarketingStream):
    """Define categories stream.

    https://developer.servicetitan.io/api-details/#api=tenant-marketing-v2&operation=CampaignCategories_GetList
    """

    name = "marketing_categories"
    path = "/categories"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(MARKETING, key="Marketing.V2.CampaignCategoryResponse")


class CostsStream(_BaseMarketingStream):
    """Define costs stream."""

    name = "costs"
    path = "/costs"
    primary_keys = ("id",)
    schema = ServiceTitanSchema(MARKETING, key="Marketing.V2.CampaignCostResponse")


class SuppressionsStream(_BaseMarketingStream, active_any=True, sort_by="ModifiedOn"):
    """Define suppressions stream.

    https://developer.servicetitan.io/api-details/#api=tenant-marketing-v2&operation=Suppressions_GetList
    """

    name = "suppressions"
    path = "/suppressions"
    primary_keys = ("email",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(MARKETING, key="Marketing.V2.SuppressionResponse")
