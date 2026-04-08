"""Job Planning and Management streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
import typing as t

from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
from tap_service_titan.openapi_specs import JPM, ServiceTitanSchema

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context, Record


class _BaseJpmStream(ServiceTitanStream, api_prefix="/jpm/v2"):
    pass


class _BaseJpmExportStream(ServiceTitanExportStream, api_prefix="/jpm/v2"):
    pass


# JPM Streams
class AppointmentsStream(_BaseJpmExportStream, active_any=True):
    """Define appointments stream."""

    name = "appointments"
    path = "/export/appointments"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ExportAppointmentsResponse")


class JobsStream(_BaseJpmExportStream):
    """Define jobs stream."""

    name = "jobs"
    path = "/export/jobs"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ExportJobsResponse")

    @override
    def get_child_context(self, record: Record, context: Context | None) -> Context:
        """Return a context dictionary for a child stream."""
        return {"job_id": record["id"]}


class JobHistoryStream(_BaseJpmExportStream):
    """Define job history stream."""

    name = "job_history"
    path = "/export/job-history"
    primary_keys = ("id",)
    replication_key = "date"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ExportJobHistoryEntry")

    # Parse the default 'data' path for response records but the real contents are in
    # the nested history array. Keep the jobID from the top level then yield
    # each history item as its own record.
    @override
    def parse_response(self, response: requests.Response) -> t.Iterable[Record]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        for record in super().parse_response(response):
            for hist_record in record.get("history", []):
                yield {"jobId": record.get("jobId"), **hist_record}


class ProjectsStream(_BaseJpmExportStream):
    """Define projects stream."""

    name = "projects"
    path = "/export/projects"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ExportProjectsResponse")


class JobCanceledLogsStream(_BaseJpmExportStream):
    """Define cancelled job stream."""

    name = "job_canceled_logs"
    path = "/export/job-canceled-logs"
    primary_keys = ("id",)
    replication_key: str = "createdOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ExportJobCanceledLogResponse")


class JobCancelReasonsStream(_BaseJpmStream):
    """Define job cancel reasons stream."""

    name = "job_cancel_reasons"
    path = "/job-cancel-reasons"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.JobCancelReasonResponse")


class JobHoldReasonsStream(_BaseJpmStream):
    """Define job hold reasons stream."""

    name = "job_hold_reasons"
    path = "/job-hold-reasons"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.JobHoldReasonResponse")


class JobTypesStream(_BaseJpmStream):
    """Define job types stream."""

    name = "job_types"
    path = "/job-types"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.JobTypeResponse")


class ProjectStatusesStream(_BaseJpmStream):
    """Define project statuses stream."""

    name = "project_statuses"
    path = "/project-statuses"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ProjectStatusResponse")


class ProjectSubStatusesStream(_BaseJpmStream):
    """Define project substatuses stream."""

    name = "project_sub_statuses"
    path = "/project-substatuses"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ProjectSubStatusResponse")


class JobNotesStream(_BaseJpmExportStream):
    """Define job notes stream."""

    name = "job_notes"
    path = "/export/job-notes"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ExportJobNotesResponse")


class ProjectNotesStream(_BaseJpmExportStream):
    """Define project notes stream."""

    name = "project_notes"
    path = "/export/project-notes"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ExportProjectNotesResponse")


class ProjectTypesStream(_BaseJpmStream):
    """Define project types stream."""

    name = "project_types"
    path = "/project-types"
    primary_keys = ("id",)
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.ProjectTypeResponse")


class JobBookedLogStream(_BaseJpmStream):
    """Define job booked log stream."""

    name = "job_booked_log"
    path = "/jobs/{job_id}/booked-log"
    primary_keys = ("id",)
    parent_stream_type = JobsStream
    ignore_parent_replication_key = True
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.JobBookedLogResponse")


class JobCanceledLogStream(_BaseJpmStream):
    """Define job canceled log stream."""

    name = "job_canceled_log"
    path = "/jobs/{job_id}/canceled-log"
    primary_keys = ("id",)
    parent_stream_type = JobsStream
    ignore_parent_replication_key = True
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.JobCanceledLogResponse")


class JobCustomFieldsStream(_BaseJpmStream):
    """Define job custom field types stream.

    https://developer.servicetitan.io/api-details/#api=tenant-jpm-v2&operation=Jobs_GetCustomFieldTypes
    """

    name = "job_custom_fields"
    path = "/jobs/custom-fields"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.CustomFieldTypeResponse")


class ProjectCustomFieldsStream(_BaseJpmStream):
    """Define project custom field types stream.

    https://developer.servicetitan.io/api-details/#api=tenant-jpm-v2&operation=Projects_GetCustomFieldTypes
    """

    name = "project_custom_fields"
    path = "/projects/custom-fields"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(JPM, key="Jpm.V2.CustomFieldTypeResponse")


class WBSBudgetCodesStream(_BaseJpmStream):
    """Define work breakdown structure budget codes stream.

    https://developer.servicetitan.io/api-details/#api=tenant-jpm-v2&operation=BudgetCodes_ListCompanyBudgetCodes
    """

    name = "wbs_budget_codes"
    path = "/work-breakdown-structure/budget-codes"
    primary_keys = ("id",)
    schema = ServiceTitanSchema(JPM, key="Jpm.BudgetCodes.BudgetCodeModel")


class WBSSegmentsStream(_BaseJpmStream):
    """Define work breakdown structure segments stream.

    https://developer.servicetitan.io/api-details/#api=tenant-jpm-v2&operation=BudgetCodes_ListCompanySegments
    """

    name = "wbs_segments"
    path = "/work-breakdown-structure/segments"
    primary_keys = ("id",)
    schema = ServiceTitanSchema(JPM, key="Jpm.BudgetCodes.SegmentModel")
