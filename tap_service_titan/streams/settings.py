"""Settings streams for the ServiceTitan tap."""

from __future__ import annotations

from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
from tap_service_titan.openapi_specs import SETTINGS, ServiceTitanSchema


class _BaseSettingsStream(ServiceTitanStream, api_prefix="/settings/v2"):
    pass


class _BaseSettingsExportStream(ServiceTitanExportStream, api_prefix="/settings/v2"):
    pass


class EmployeesStream(_BaseSettingsExportStream):
    """Define employees stream."""

    name = "employees"
    path = "/export/employees"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(SETTINGS, key="TenantSettings.V2.ExportEmployeeResponse")


class BusinessUnitsStream(_BaseSettingsExportStream):
    """Define business units stream."""

    name = "business_units"
    path = "/export/business-units"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(SETTINGS, key="TenantSettings.V2.ExportBusinessUnitResponse")


class TechniciansStream(_BaseSettingsExportStream):
    """Define technicians stream."""

    name = "technicians"
    path = "/export/technicians"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(SETTINGS, key="TenantSettings.V2.ExportTechnicianResponse")


class TagTypesStream(_BaseSettingsStream):
    """Define tag types stream."""

    name = "tag_types"
    path = "/tag-types"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(SETTINGS, key="TenantSettings.V2.TagTypeResponse")


class UserRolesStream(_BaseSettingsStream):
    """Define user roles stream."""

    name = "user_roles"
    path = "/user-roles"
    primary_keys = ("id",)
    replication_key: str = "createdOn"
    schema = ServiceTitanSchema(SETTINGS, key="TenantSettings.V2.UserRoleResponse")


class IntacctBusinessUnitMappingsStream(
    _BaseSettingsStream,
    page_size=500,
    include_total=True,  # Parameter is required by this endpoint
    first_page=0,  # UNDOCUMENTED: Pagination starts at page 0
):
    """Define Intacct business unit mappings stream."""

    name = "intacct_business_unit_mappings"
    path = "/business-units/intacct"
    primary_keys = ("id",)
    schema = ServiceTitanSchema(
        SETTINGS,
        key="Accounting.V2.Integrations.Intacct.IntacctBusinessUnitMappingResponse",
    )
