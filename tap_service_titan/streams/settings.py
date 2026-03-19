"""Settings streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
from functools import cached_property

from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
from tap_service_titan.openapi_specs import SETTINGS, ServiceTitanSchema

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class EmployeesStream(ServiceTitanExportStream):
    """Define employees stream."""

    name = "employees"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(SETTINGS, key="TenantSettings.V2.ExportEmployeeResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/settings/v2/tenant/{self.tenant_id}/export/employees"


class BusinessUnitsStream(ServiceTitanExportStream):
    """Define business units stream."""

    name = "business_units"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(SETTINGS, key="TenantSettings.V2.ExportBusinessUnitResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/settings/v2/tenant/{self.tenant_id}/export/business-units"


class TechniciansStream(ServiceTitanExportStream):
    """Define technicians stream."""

    name = "technicians"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(SETTINGS, key="TenantSettings.V2.ExportTechnicianResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/settings/v2/tenant/{self.tenant_id}/export/technicians"


class TagTypesStream(ServiceTitanStream):
    """Define tag types stream."""

    name = "tag_types"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(SETTINGS, key="TenantSettings.V2.TagTypeResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/settings/v2/tenant/{self.tenant_id}/tag-types"


class UserRolesStream(ServiceTitanStream):
    """Define user roles stream."""

    name = "user_roles"
    primary_keys = ("id",)
    replication_key: str = "createdOn"
    schema = ServiceTitanSchema(SETTINGS, key="TenantSettings.V2.UserRoleResponse")

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/settings/v2/tenant/{self.tenant_id}/user-roles"


class IntacctBusinessUnitMappingsStream(ServiceTitanStream):
    """Define Intacct business unit mappings stream."""

    name = "intacct_business_unit_mappings"
    primary_keys = ("id",)
    schema = ServiceTitanSchema(
        SETTINGS,
        key="Accounting.V2.Integrations.Intacct.IntacctBusinessUnitMappingResponse",
    )

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/settings/v2/tenant/{self.tenant_id}/business-units/intacct"
