"""Equipment systems streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
from functools import cached_property

from tap_service_titan.client import ServiceTitanStream
from tap_service_titan.openapi_specs import EQUIPMENT_SYSTEMS, ServiceTitanSchema

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


class InstalledEquipmentStream(ServiceTitanStream):
    """Define installed equipment stream."""

    name = "installed_equipment"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        EQUIPMENT_SYSTEMS,
        key="EquipmentSystems.V2.InstalledEquipmentResponse",
    )

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/equipmentsystems/v2/tenant/{self.tenant_id}/installed-equipment"


class EquipmentTypesStream(ServiceTitanStream, active_any=True):
    """Define equipment types stream.

    https://developer.servicetitan.io/api-details/#api=tenant-equipment-systems-v2&operation=EquipmentTypes_GetList
    """

    name = "equipment_types"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        EQUIPMENT_SYSTEMS,
        key="EquipmentSystems.V2.EquipmentTypeResponse",
    )

    @override
    @cached_property
    def path(self) -> str:
        """Return the API path for the stream."""
        return f"/equipmentsystems/v2/tenant/{self.tenant_id}/equipment-types"
