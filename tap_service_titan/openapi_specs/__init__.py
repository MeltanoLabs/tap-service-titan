"""OpenAPI specs for the ServiceTitan tap."""

from __future__ import annotations

import logging
import sys
from functools import cached_property
from importlib.resources import files
from typing import TYPE_CHECKING, Any

from singer_sdk import OpenAPISchema, StreamSchema

if TYPE_CHECKING:
    from singer_sdk.streams import Stream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

__all__ = [
    "ACCOUNTING",
    "CRM",
    "CUSTOMER_INTERACTIONS",
    "DISPATCH",
    "EQUIPMENT_SYSTEMS",
    "FORMS",
    "INVENTORY",
    "JBCE",
    "JPM",
    "MARKETING_ADS",
    "MARKETING_REPUTATION",
    "MEMBERSHIPS",
    "PAYROLL",
    "PRICEBOOK",
    "SALESTECH",
    "SCHEDULING_PRO",
    "SERVICE_AGREEMENTS",
    "SETTINGS",
    "TASK_MANAGEMENT",
    "TELECOM",
]

logger = logging.getLogger(__name__)


class ServiceTitanOpenAPISchema(OpenAPISchema):
    """ServiceTitan OpenAPI schema."""

    @override
    @cached_property
    def spec(self) -> dict[str, Any]:
        """Get the spec."""
        spec = super().spec
        schemas = spec["components"]["schemas"]

        # Accounting
        for comp in (
            "Accounting.V2.JournalEntryResponse",
            "Accounting.V2.JournalEntrySummaryResponse",
            "Accounting.V2.JournalEntryDetailsResponse",
        ):
            if comp in schemas:
                props = schemas[comp]["properties"]
                # Values are coming in as '2025-10-13T00:00:00', so not valid ISO dates
                props["postDate"]["format"] = "date-time"

        return spec


class ServiceTitanSchema(StreamSchema):
    """ServiceTitan schema."""

    @override
    def get_stream_schema(
        self,
        stream: Stream,
        stream_class: type[Stream],
    ) -> dict[str, Any]:
        """Get the schema for the given stream."""
        schema = super().get_stream_schema(stream, stream_class)

        if stream.name == "job_history":
            schema["required"].append("jobId")
            schema["properties"]["jobId"] = {
                "description": "ID of the job",
                "format": "int64",
                "type": "integer",
            }

        if stream.name == "estimate_items":
            schema["properties"]["estimate_id"] = {
                "format": "int64",
                "type": "integer",
            }

        if stream.name == "categories":
            schema["required"].remove("modifiedOn")

        if stream.name in ("campaign_performance", "keyword_performance", "adgroup_performance"):
            # TODO(maintainers): Remove these `status` patches once ServiceTitan fixes this.
            # https://github.com/archdotdev/tap-service-titan/issues/163
            schema["properties"]["campaign"]["properties"]["status"]["type"] = [
                "integer",
                "null",
            ]
            schema["properties"]["adGroup"]["properties"]["status"]["type"] = [
                "integer",
                "null",
            ]
            schema["properties"]["keyword"]["properties"]["status"]["type"] = [
                "integer",
                "null",
            ]
            schema["properties"] |= {
                "date": {
                    "type": ["string", "null"],
                    "format": "date",
                },
                "from_utc": {
                    "type": ["string", "null"],
                    "format": "date-time",
                },
                "to_utc": {
                    "type": ["string", "null"],
                    "format": "date-time",
                },
                "campaign_id": schema["properties"]["campaign"]["properties"]["id"],
                "campaign_name": schema["properties"]["campaign"]["properties"]["name"],
                "adGroup_id": schema["properties"]["adGroup"]["properties"]["id"],
                "keyword_id": schema["properties"]["keyword"]["properties"]["id"],
            }

        if stream.name == "bank_deposit_transactions":
            schema["properties"]["bankDepositId"] = {
                "format": "int64",
                "type": "integer",
            }

        if stream.name in ("inventory_bills", "purchase_order_types", "receipts"):
            # TODO(maintainers): Remove once ServiceTitan fixes this.
            # API returns integer enum values (e.g. 0) instead of string names.
            # https://github.com/MeltanoLabs/tap-service-titan/issues/358
            schema["properties"]["landedCostMethod"]["type"] = [
                "integer",
                "string",
                "null",
            ]

        return schema


SPECS = files("tap_service_titan") / "openapi_specs"
ACCOUNTING = ServiceTitanOpenAPISchema(SPECS / "accounting-v2.json")
CRM = ServiceTitanOpenAPISchema(SPECS / "crm-v2.json")
CUSTOMER_INTERACTIONS = ServiceTitanOpenAPISchema(SPECS / "customer-interactions-v2.json")
DISPATCH = ServiceTitanOpenAPISchema(SPECS / "dispatch-v2.json")
EQUIPMENT_SYSTEMS = ServiceTitanOpenAPISchema(SPECS / "equipment-systems-v2.json")
FORMS = ServiceTitanOpenAPISchema(SPECS / "forms-v2.json")
INVENTORY = ServiceTitanOpenAPISchema(SPECS / "inventory-v2.json")
JBCE = ServiceTitanOpenAPISchema(SPECS / "jbce-v2.json")
JPM = ServiceTitanOpenAPISchema(SPECS / "jpm-v2.json")
MARKETING = ServiceTitanOpenAPISchema(SPECS / "marketing-v2.json")
MARKETING_ADS = ServiceTitanOpenAPISchema(SPECS / "marketing-ads-v2.json")
MARKETING_REPUTATION = ServiceTitanOpenAPISchema(SPECS / "marketing-reputation-v2.json")
MEMBERSHIPS = ServiceTitanOpenAPISchema(SPECS / "memberships-v2.json")
PAYROLL = ServiceTitanOpenAPISchema(SPECS / "payroll-v2.json")
PRICEBOOK = ServiceTitanOpenAPISchema(SPECS / "pricebook-v2.json")
SALESTECH = ServiceTitanOpenAPISchema(SPECS / "salestech-v2.json")
SCHEDULING_PRO = ServiceTitanOpenAPISchema(SPECS / "scheduling-pro-v2.json")
SERVICE_AGREEMENTS = ServiceTitanOpenAPISchema(SPECS / "service-agreements-v2.json")
SETTINGS = ServiceTitanOpenAPISchema(SPECS / "settings-v2.json")
TASK_MANAGEMENT = ServiceTitanOpenAPISchema(SPECS / "task-management-v2.json")
TELECOM = ServiceTitanOpenAPISchema(SPECS / "telecom.json")
