"""Job Planning and Management streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
import typing as t
from functools import cached_property

from tap_service_titan.client import (
    ServiceTitanExportStream,
    ServiceTitanStream,
)
from tap_service_titan.openapi_specs import JPM, ServiceTitanSchema

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context, Record


# JPM Streams
class AppointmentsStream(ServiceTitanExportStream, active_any=True):
    """Define appointments stream."""

    name = "appointments"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ExportAppointmentsResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/export/appointments"


class JobsStream(ServiceTitanExportStream):
    """Define jobs stream."""

    name = "jobs"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ExportJobsResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/export/jobs"

    @override
    def get_child_context(self, record: Record, context: Context | None) -> Context:
        """Return a context dictionary for a child stream."""
        return {"job_id": record["id"]}


class JobHistoryStream(ServiceTitanExportStream):
    """Define job history stream."""

    name = "job_history"
    primary_keys = ("id",)
    replication_key = "date"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ExportJobHistoryEntry")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/export/job-history"

    # Parse the default 'data' path for response records but the real contents are in
    # the nested history array. Keep the jobID from the top level then yield
    # each history item as its own record.
    @override
    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        for record in super().parse_response(response):
            for hist_record in record.get("history", []):
                yield {"jobId": record.get("jobId"), **hist_record}


class ProjectsStream(ServiceTitanExportStream):
    """Define projects stream."""

    name = "projects"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ExportProjectsResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/export/projects"


class JobCanceledLogsStream(ServiceTitanExportStream):
    """Define cancelled job stream."""

    name = "job_canceled_logs"
    primary_keys = ("id",)
    replication_key: str = "createdOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ExportJobCanceledLogResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/export/job-canceled-logs"


class JobCancelReasonsStream(ServiceTitanStream):
    """Define job cancel reasons stream."""

    name = "job_cancel_reasons"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.JobCancelReasonResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/job-cancel-reasons"


class JobHoldReasonsStream(ServiceTitanStream):
    """Define job hold reasons stream."""

    name = "job_hold_reasons"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.JobHoldReasonResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/job-hold-reasons"


class JobTypesStream(ServiceTitanStream):
    """Define job types stream."""

    name = "job_types"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.JobTypeResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/job-types"


class ProjectStatusesStream(ServiceTitanStream):
    """Define project statuses stream."""

    name = "project_statuses"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ProjectStatusResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/project-statuses"


class ProjectSubStatusesStream(ServiceTitanStream):
    """Define project substatuses stream."""

    name = "project_sub_statuses"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ProjectSubStatusResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/project-substatuses"


class JobNotesStream(ServiceTitanExportStream):
    """Define job notes stream."""

    name = "job_notes"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ExportJobNotesResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/export/job-notes"


class ProjectNotesStream(ServiceTitanExportStream):
    """Define project notes stream."""

    name = "project_notes"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ExportProjectNotesResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/export/project-notes"


class ProjectTypesStream(ServiceTitanStream):
    """Define project types stream."""

    name = "project_types"
    primary_keys = ("id",)
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ProjectTypeResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/project-types"


class JobBookedLogStream(ServiceTitanStream):
    """Define job booked log stream."""

    name = "job_booked_log"
    primary_keys = ("id",)
    parent_stream_type = JobsStream
    ignore_parent_replication_key = True
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.JobBookedLogResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/jobs/{{job_id}}/booked-log"


class JobCanceledLogStream(ServiceTitanStream):
    """Define job canceled log stream."""

    name = "job_canceled_log"
    primary_keys = ("id",)
    parent_stream_type = JobsStream
    ignore_parent_replication_key = True
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.JobCanceledLogResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/jobs/{{job_id}}/canceled-log"


class JobCustomFieldsStream(ServiceTitanStream):
    """Define job custom field types stream.

    https://developer.servicetitan.io/api-details/#api=tenant-jpm-v2&operation=Jobs_GetCustomFieldTypes
    """

    name = "job_custom_fields"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.CustomFieldTypeResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/jobs/custom-fields"


class ProjectCustomFieldsStream(ServiceTitanStream):
    """Define project custom field types stream.

    https://developer.servicetitan.io/api-details/#api=tenant-jpm-v2&operation=Projects_GetCustomFieldTypes
    """

    name = "project_custom_fields"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.CustomFieldTypeResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/projects/custom-fields"


class WBSBudgetCodesStream(ServiceTitanStream):
    """Define work breakdown structure budget codes stream.

    https://developer.servicetitan.io/api-details/#api=tenant-jpm-v2&operation=BudgetCodes_ListCompanyBudgetCodes
    """

    name = "wbs_budget_codes"
    primary_keys = ("id",)
    schema = ServiceTitanSchema(JPM, key="Jpm.BudgetCodes.BudgetCodeModel")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/work-breakdown-structure/budget-codes"


class WBSSegmentsStream(ServiceTitanStream):
    """Define work breakdown structure segments stream.

    https://developer.servicetitan.io/api-details/#api=tenant-jpm-v2&operation=BudgetCodes_ListCompanySegments
    """

    name = "wbs_segments"
    primary_keys = ("id",)
    schema = ServiceTitanSchema(JPM, key="Jpm.BudgetCodes.SegmentModel")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/jpm/v2/tenant/{self.tenant_id}/work-breakdown-structure/segments"
