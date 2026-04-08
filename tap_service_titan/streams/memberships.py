"""Memberships streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
from tap_service_titan.openapi_specs import MEMBERSHIPS, ServiceTitanSchema

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record


class _BaseMembershipsStream(ServiceTitanStream, api_prefix="/memberships/v2"):
    pass


class _BaseMembershipsExportStream(ServiceTitanExportStream, api_prefix="/memberships/v2"):
    pass


class MembershipsStream(_BaseMembershipsExportStream):
    """Define memberships export stream."""

    name = "memberships_export"
    path = "/export/memberships"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        MEMBERSHIPS,
        key="Memberships.V2.ExportCustomerMembershipResponse",
    )


class MembershipTypesStream(_BaseMembershipsExportStream):
    """Define membership types stream."""

    name = "membership_types"
    path = "/export/membership-types"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        MEMBERSHIPS,
        key="Memberships.V2.ExportMembershipTypeResponse",
    )

    @override
    def post_process(
        self,
        row: Record,
        context: Context | None = None,
    ) -> Record | None:
        # Remove null items from durationBilling
        row["durationBilling"] = [item for item in row["durationBilling"] if item is not None]
        return row


class RecurringServiceTypesStream(_BaseMembershipsExportStream):
    """Define recurring service types stream."""

    name = "recurring_service_types"
    path = "/export/recurring-service-types"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        MEMBERSHIPS,
        key="Memberships.V2.ExportRecurringServiceTypeResponse",
    )


class InvoiceTemplatesStream(_BaseMembershipsExportStream):
    """Define invoice templates stream."""

    name = "invoice_templates"
    path = "/export/invoice-templates"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        MEMBERSHIPS,
        key="Memberships.V2.ExportInvoiceTemplateResponse",
    )


class RecurringServicesStream(_BaseMembershipsExportStream):
    """Define recurring services stream."""

    name = "recurring_services"
    path = "/export/recurring-services"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        MEMBERSHIPS,
        key="Memberships.V2.ExportLocationRecurringServiceResponse",
    )


class RecurringServiceEventsStream(_BaseMembershipsExportStream):
    """Define recurring service events stream."""

    name = "recurring_service_events"
    path = "/export/recurring-service-events"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        MEMBERSHIPS,
        key="Memberships.V2.ExportLocationRecurringServiceEventResponse",
    )


class MembershipStatusChangesStream(_BaseMembershipsExportStream):
    """Define membership status changes export stream."""

    name = "membership_status_changes"
    path = "/export/membership-status-changes"
    primary_keys = ("id",)
    replication_key: str = "createdOn"
    schema = ServiceTitanSchema(
        MEMBERSHIPS,
        key="Memberships.V2.ExportCustomerMembershipStatusChangesResponse",
    )


class MembershipCustomFieldsStream(_BaseMembershipsStream):
    """Define memberships custom field types stream.

    https://developer.servicetitan.io/api-details/#api=tenant-memberships-v2&operation=CustomerMemberships_GetCustomFields
    """

    name = "membership_custom_fields"
    path = "/memberships/custom-fields"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        MEMBERSHIPS,
        key="Memberships.V2.CustomFieldTypeResponse",
    )
