"""Customer interactions streams for the ServiceTitan tap."""

from __future__ import annotations

from tap_service_titan.client import ServiceTitanExportStream
from tap_service_titan.openapi_specs import CUSTOMER_INTERACTIONS, ServiceTitanSchema


class _BaseCustomerInteractionsStream(
    ServiceTitanExportStream,
    api_prefix="/customer-interactions/v2",
):
    pass


class TechnicianRatingsStream(_BaseCustomerInteractionsStream):
    """Define technician ratings stream."""

    name = "technician_ratings"
    path = "/export/technician-ratings"
    primary_keys = ("technicianId", "jobId")
    replication_key = "modifiedOn"
    schema = ServiceTitanSchema(
        CUSTOMER_INTERACTIONS,
        key="CustomerInteractions.V2.ExportTechnicianAssessmentResponse",
    )
