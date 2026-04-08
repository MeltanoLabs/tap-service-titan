"""Marketing ads streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any

from tap_service_titan._common import now
from tap_service_titan.client import DateRange, DateRangePaginator, ServiceTitanStream
from tap_service_titan.openapi_specs import MARKETING_ADS, ServiceTitanSchema

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers import types


class _BaseMarketingAdsStream(ServiceTitanStream[DateRange], api_prefix="/marketingads/v2"):
    pass


class AttributedLeadsStream(_BaseMarketingAdsStream):
    """Define attributed leads stream."""

    name = "attributed_leads"
    path = "/attributed-leads"
    primary_keys: tuple[str] = ("dateTime",)
    replication_key: str = "dateTime"
    schema = ServiceTitanSchema(
        MARKETING_ADS,
        # https://developer.servicetitan.io/api-details/#api=tenant-marketing-ads-v2&operation=AttributedLeads_Get&definition=Marketing.Ads.Contracts.AttributedLeads.GetAttributedLeadsResponse
        key="Marketing.Ads.Contracts.AttributedLeads.GetAttributedLeadsResponse",
    )

    @override
    def get_url_params(
        self,
        context: types.Context | None,
        next_page_token: Any | None,
    ) -> dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["fromUtc"] = params.pop("modifiedOnOrAfter")
        params["toUtc"] = now().isoformat()
        return params


class CapacityWarningsStream(_BaseMarketingAdsStream):
    """Define capacity warnings stream."""

    name = "capacity_warnings"
    path = "/capacity-warnings"
    primary_keys: tuple[str, str] = ("campaignName", "warningType")
    schema = ServiceTitanSchema(
        MARKETING_ADS,
        # https://developer.servicetitan.io/api-details/#api=tenant-marketing-ads-v2&operation=CapacityAwarenessWarning_Get&definition=Marketing.Ads.Client.CapacityAwarenessWarning
        key="Marketing.Ads.Client.CapacityAwarenessWarning",
    )


class _PerformanceStream(_BaseMarketingAdsStream):
    """Define marketing performance stream."""

    name = "performance"
    path = "/performance"
    primary_keys: tuple[str, ...] = ()
    replication_key: str = "from_utc"

    schema = ServiceTitanSchema(
        MARKETING_ADS,
        # https://developer.servicetitan.io/api-details/#api=tenant-marketing-ads-v2&operation=Performance_Get&definition=Marketing.Ads.Contracts.Performance.GetPerformanceResponse
        key="Marketing.Ads.Contracts.Performance.GetPerformanceResponse",
    )

    @override
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Add report end time for consistency."""
        super().__init__(*args, **kwargs)
        self.end_time = now()
        self.interval = timedelta(days=1)
        self._paginator: DateRangePaginator | None = None

    @property
    def paginator(self) -> DateRangePaginator:
        """Get the paginator for the stream."""
        if self._paginator is None:
            msg = "Paginator not initialized"
            raise RuntimeError(msg)
        return self._paginator

    def _get_default_start_date(self) -> datetime:
        """Get default start date when none is provided."""
        return now() - timedelta(days=30)

    def _get_effective_start_date(self, context: types.Context | None = None) -> datetime:
        """Get the effective start date for the current context."""
        if start_date := self.get_starting_timestamp(context):
            effective_start_date = start_date
        else:
            effective_start_date = self._get_default_start_date()

        return effective_start_date.replace(hour=0, minute=0, second=0, microsecond=0)

    @override
    def post_process(
        self,
        row: types.Record,
        context: types.Context | None = None,
    ) -> types.Record | None:
        """Process the record to add top-level IDs.

        Args:
            row: Individual record in the stream.
            context: Stream partition or context dictionary.

        Returns:
            The resulting record dict, or `None` if the record should be excluded.
        """
        row["campaign_id"] = (row.get("campaign") or {}).get("id")
        row["campaign_name"] = (row.get("campaign") or {}).get("name")
        row["adGroup_id"] = (row.get("adGroup") or {}).get("id")
        row["keyword_id"] = (row.get("keyword") or {}).get("id")
        row["date"] = self.paginator.current_value.start.date()
        row["from_utc"] = self.paginator.current_value.start
        row["to_utc"] = self.paginator.current_value.end
        return row

    @override
    def get_new_paginator(self) -> DateRangePaginator:
        """Create a new pagination helper instance for date ranges."""
        start_date = self._get_effective_start_date(self.context)
        self._paginator = DateRangePaginator(start_date, self.interval, self.end_time)
        return self._paginator

    @override
    def get_url_params(
        self,
        context: types.Context | None,
        next_page_token: DateRange | None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {}

        if next_page_token is None:
            msg = "Paginator not initialized"
            raise RuntimeError(msg)

        params["fromUtc"] = next_page_token.start.isoformat()
        params["toUtc"] = next_page_token.end.isoformat()
        params["pageSize"] = 5000
        return params


class CampaignPerformanceStream(_PerformanceStream):
    """Define marketing performance stream for campaigns."""

    name = "campaign_performance"
    primary_keys: tuple[str, str] = ("campaign_id", "date")

    @override
    def get_url_params(
        self,
        context: types.Context | None,
        next_page_token: DateRange | None,
    ) -> dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["performanceSegmentationType"] = "Campaign"
        return params


class KeywordPerformanceStream(_PerformanceStream):
    """Define marketing performance stream for campaigns."""

    name = "keyword_performance"
    primary_keys: tuple[str, str] = ("keyword_id", "date")

    @override
    def get_url_params(
        self,
        context: types.Context | None,
        next_page_token: DateRange | None,
    ) -> dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["performanceSegmentationType"] = "Keyword"
        return params


class AdGroupPerformanceStream(_PerformanceStream):
    """Define marketing performance stream for campaigns."""

    name = "adgroup_performance"
    primary_keys: tuple[str, str] = ("adGroup_id", "date")

    @override
    def get_url_params(
        self,
        context: types.Context | None,
        next_page_token: DateRange | None,
    ) -> dict[str, Any]:
        params = super().get_url_params(context, next_page_token)
        params["performanceSegmentationType"] = "AdGroup"
        return params
