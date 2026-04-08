"""Payroll streams for the ServiceTitan tap."""

from __future__ import annotations

from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
from tap_service_titan.openapi_specs import PAYROLL, ServiceTitanSchema


class _BasePayrollStream(ServiceTitanStream, api_prefix="/payroll/v2"):
    pass


class _BasePayrollExportStream(ServiceTitanExportStream, api_prefix="/payroll/v2"):
    pass


class JobSplitsStream(_BasePayrollExportStream):
    """Define job splits stream."""

    name = "job_splits"
    path = "/export/jobs/splits"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(PAYROLL, key="Payroll.V2.JobSplits.JobSplitExportResponse")


class PayrollAdjustmentsStream(_BasePayrollExportStream):
    """Define payroll adjustments stream."""

    name = "payroll_adjustments"
    path = "/export/payroll-adjustments"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        PAYROLL,
        key="Payroll.V2.PayrollAdjustments.PayrollAdjustmentExportResponse",
    )


class JobTimesheetsStream(_BasePayrollExportStream):
    """Define job timesheets stream."""

    name = "job_timesheets"
    path = "/export/jobs/timesheets"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        PAYROLL,
        key="Payroll.V2.Timesheets.JobTimesheetExportResponse",
    )


class ActivityCodesStream(_BasePayrollExportStream):
    """Define activity codes stream."""

    name = "activity_codes"
    path = "/export/activity-codes"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        PAYROLL,
        key="Payroll.V2.PayrollActivityCodes.PayrollActivityCodeExportResponse",
    )


class TimesheetCodesStream(_BasePayrollExportStream):
    """Define timesheet codes stream."""

    name = "timesheet_codes"
    path = "/export/timesheet-codes"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        PAYROLL,
        key="Payroll.V2.TimesheetCodes.TimesheetCodeExportResponse",
    )


class GrossPayItemsStream(_BasePayrollExportStream):
    """Define gross pay items stream."""

    name = "gross_pay_items"
    path = "/export/gross-pay-items"
    primary_keys = ("payrollId", "date")
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        PAYROLL,
        key="Payroll.V2.GrossPayItems.GrossPayItemExportResponse",
    )


class LocationRatesStream(_BasePayrollStream, active_any=True):
    """Define location rates stream.

    https://developer.servicetitan.io/api-details/#api=tenant-payroll-v2&operation=LocationLaborType_GetListByLocations
    """

    name = "location_rates"
    path = "/locations/rates"
    primary_keys = ("locationId", "laborTypeCode")
    schema = ServiceTitanSchema(
        PAYROLL,
        key="Payroll.V2.LocationLaborTypes.LocationLaborTypeResponse",
    )


class PayrollsStream(_BasePayrollStream, active_any=True):
    """Define payrolls stream.

    https://developer.servicetitan.io/api-details/#api=tenant-payroll-v2&operation=Payrolls_GetList
    """

    name = "payrolls"
    path = "/payrolls"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(PAYROLL, key="Payroll.V2.Payrolls.PayrollResponse")


class PayrollSettingsStream(_BasePayrollStream, active_any=True):
    """Payroll settings.

    https://developer.servicetitan.io/api-details/#api=tenant-payroll-v2&operation=PayrollSettings_GetPayrollSettingsList
    """

    name = "payroll_settings"
    path = "/payroll-settings"
    primary_keys = ("employeeId",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        PAYROLL,
        key="Payroll.V2.PayrollSettings.PayrollSettingsListResponse",
    )


class NonJobTimesheetsStream(_BasePayrollStream, active_any=True):
    """Define non-job timesheets stream.

    https://developer.servicetitan.io/api-details/#api=tenant-payroll-v2&operation=Timesheets_GetNonJobTimesheets
    """

    name = "non_job_timesheets"
    path = "/non-job-timesheets"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        PAYROLL,
        key="Payroll.V2.Timesheets.NonJobTimesheetResponse",
    )
