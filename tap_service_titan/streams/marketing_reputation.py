"""Marketing reputation streams for the ServiceTitan tap."""

from __future__ import annotations

from tap_service_titan.client import ServiceTitanStream
from tap_service_titan.openapi_specs import MARKETING_REPUTATION, ServiceTitanSchema


class _BaseMarketingReputationStream(ServiceTitanStream, api_prefix="/marketingreputation/v2"):
    pass


class ReviewsStream(_BaseMarketingReputationStream):
    """Define reviews stream."""

    name = "reviews"
    path = "/reviews"
    primary_keys = ("review", "platform", "address")
    replication_key: str = "publishDate"
    schema = ServiceTitanSchema(
        MARKETING_REPUTATION,
        key="ServiceTitan.Marketing.ReviewEngine.Services.Models.Reviews.ReviewReport",
    )
