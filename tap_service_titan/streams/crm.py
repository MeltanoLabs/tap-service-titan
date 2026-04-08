"""CRM streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
from tap_service_titan.openapi_specs import CRM, ServiceTitanSchema

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record


class _BaseCrmStream(ServiceTitanStream, api_prefix="/crm/v2"):
    pass


class _BaseCrmExportStream(ServiceTitanExportStream, api_prefix="/crm/v2"):
    pass


# CRM Streams
class BookingProviderTagsStream(_BaseCrmStream):
    """Define booking provider tags stream."""

    name = "booking_provider_tags"
    path = "/booking-provider-tags"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(CRM, key="Crm.V2.BookingProviderTagResponse")


class BookingsStream(_BaseCrmExportStream):
    """Define bookings stream."""

    name = "bookings"
    path = "/export/bookings"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(CRM, key="Crm.V2.ExportBookingResponse")


class CustomersStream(_BaseCrmExportStream):
    """Define customers stream."""

    name = "customers"
    path = "/export/customers"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(CRM, key="Crm.V2.ExportCustomerResponse")

    @override
    def get_child_context(self, record: Record, context: Context | None) -> Context:
        """Return a context dictionary for a child stream."""
        return {"customer_id": record["id"]}


class CustomerNotesStream(_BaseCrmStream):
    """Define customer notes stream."""

    name = "customer_notes"
    path = "/customers/{customer_id}/notes"
    primary_keys = ("createdById", "createdOn")
    replication_key: str = "modifiedOn"
    parent_stream_type = CustomersStream
    ignore_parent_replication_key = True
    schema = ServiceTitanSchema(CRM, key="Crm.V2.NoteResponse")


class CustomerContactsStream(_BaseCrmExportStream):
    """Define contacts stream."""

    name = "customer_contacts"
    path = "/export/customers/contacts"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(CRM, key="Crm.V2.ExportCustomerContactResponse")


class LeadsStream(_BaseCrmExportStream):
    """Define leads stream."""

    name = "leads"
    path = "/export/leads"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(CRM, key="Crm.V2.ExportLeadsResponse")

    @override
    def get_child_context(self, record: Record, context: Context | None) -> Context:
        """Return a context dictionary for a child stream."""
        return {"lead_id": record["id"]}


class LeadNotesStream(_BaseCrmStream):
    """Define lead notes stream."""

    name = "lead_notes"
    path = "/leads/{lead_id}/notes"
    primary_keys = ("createdById", "createdOn")
    replication_key: str = "modifiedOn"
    parent_stream_type = LeadsStream
    ignore_parent_replication_key = True
    schema = ServiceTitanSchema(CRM, key="Crm.V2.NoteResponse")


class LocationsStream(_BaseCrmExportStream):
    """Define locations stream."""

    name = "locations"
    path = "/export/locations"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(CRM, key="Crm.V2.ExportLocationsResponse")

    @override
    def get_child_context(self, record: Record, context: Context | None) -> Context:
        """Return a context dictionary for a child stream."""
        return {"location_id": record["id"]}


class LocationNotesStream(_BaseCrmStream):
    """Define location notes stream."""

    name = "location_notes"
    path = "/locations/{location_id}/notes"
    primary_keys = ("createdById", "createdOn")
    replication_key: str = "modifiedOn"
    parent_stream_type = LocationsStream
    ignore_parent_replication_key = True
    schema = ServiceTitanSchema(CRM, key="Crm.V2.NoteResponse")


class LocationContactsStream(_BaseCrmExportStream):
    """Define location contacts stream."""

    name = "location_contacts"
    path = "/export/locations/contacts"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(CRM, key="Crm.V2.ExportLocationContactResponse")


class LocationsCustomFieldsStream(_BaseCrmStream):
    """Define locations custom fields stream."""

    name = "locations_custom_fields"
    path = "/locations/custom-fields"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(CRM, key="Crm.V2.Locations.CustomFieldTypeResponse")


class CustomersCustomFieldsStream(_BaseCrmStream):
    """Define customers custom fields stream."""

    name = "customers_custom_fields"
    path = "/customers/custom-fields"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(CRM, key="Crm.V2.Customers.CustomFieldTypeResponse")
