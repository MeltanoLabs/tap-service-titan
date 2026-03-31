"""Memberships streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
from functools import cached_property
from typing import TYPE_CHECKING

from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
from tap_service_titan.openapi_specs import MEMBERSHIPS, ServiceTitanSchema

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record


class MembershipsStream(ServiceTitanExportStream):
    """Define memberships export stream."""

    name = "memberships_export"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        MEMBERSHIPS,
        key="Memberships.V2.ExportCustomerMembershipResponse",
    )

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/memberships/v2/tenant/{self.tenant_id}/export/memberships"


class MembershipTypesStream(ServiceTitanExportStream):
    """Define membership types stream."""

    name = "membership_types"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        MEMBERSHIPS,
        key="Memberships.V2.ExportMembershipTypeResponse",
    )

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/memberships/v2/tenant/{self.tenant_id}/export/membership-types"

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        # Remove null items from durationBilling
        row["durationBilling"] = [item for item in row["durationBilling"] if item is not None]
        return row


class RecurringServiceTypesStream(ServiceTitanExportStream):
    """Define recurring service types stream."""

    name = "recurring_service_types"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        MEMBERSHIPS,
        key="Memberships.V2.ExportRecurringServiceTypeResponse",
    )

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/memberships/v2/tenant/{self.tenant_id}/export/recurring-service-types"


class InvoiceTemplatesStream(ServiceTitanExportStream):
    """Define invoice templates stream."""

    name = "invoice_templates"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        MEMBERSHIPS,
        key="Memberships.V2.ExportInvoiceTemplateResponse",
    )

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/memberships/v2/tenant/{self.tenant_id}/export/invoice-templates"


class RecurringServicesStream(ServiceTitanExportStream):
    """Define recurring services stream."""

    name = "recurring_services"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        MEMBERSHIPS,
        key="Memberships.V2.ExportLocationRecurringServiceResponse",
    )

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/memberships/v2/tenant/{self.tenant_id}/export/recurring-services"


class RecurringServiceEventsStream(ServiceTitanExportStream):
    """Define recurring service events stream."""

    name = "recurring_service_events"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        MEMBERSHIPS,
        key="Memberships.V2.ExportLocationRecurringServiceEventResponse",
    )

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/memberships/v2/tenant/{self.tenant_id}/export/recurring-service-events"


class MembershipStatusChangesStream(ServiceTitanExportStream):
    """Define membership status changes export stream."""

    name = "membership_status_changes"
    primary_keys = ("id",)
    replication_key: str = "createdOn"
    schema = ServiceTitanSchema(
        MEMBERSHIPS,
        key="Memberships.V2.ExportCustomerMembershipStatusChangesResponse",
    )

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/memberships/v2/tenant/{self.tenant_id}/export/membership-status-changes"


class MembershipCustomFieldsStream(ServiceTitanStream):
    """Define memberships custom field types stream.

    https://developer.servicetitan.io/api-details/#api=tenant-memberships-v2&operation=CustomerMemberships_GetCustomFields
    """

    name = "membership_custom_fields"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        MEMBERSHIPS,
        key="Memberships.V2.CustomFieldTypeResponse",
    )

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/memberships/v2/tenant/{self.tenant_id}/memberships/custom-fields"
