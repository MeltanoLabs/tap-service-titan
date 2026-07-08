"""Equipment systems streams for the ServiceTitan tap."""

from __future__ import annotations

from tap_service_titan.client import ServiceTitanExportStream, ServiceTitanStream
from tap_service_titan.openapi_specs import EQUIPMENT_SYSTEMS, ServiceTitanSchema


class _BaseEquipmentSystemsStream(ServiceTitanStream, api_prefix="/equipmentsystems/v2"):
    pass


class _BaseEquipmentSystemsExportStream(
    ServiceTitanExportStream,
    api_prefix="/equipmentsystems/v2",
):
    pass


class EquipmentSystemsStream(_BaseEquipmentSystemsExportStream):
    """Define equipment systems stream.

    https://developer.servicetitan.io/api-details/#api=tenant-equipment-systems-v2&operation=EquipmentSystems_GetExport
    """

    name = "equipment_systems"
    path = "/export/equipment-systems"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        EQUIPMENT_SYSTEMS,
        key="EquipmentSystems.V2.EquipmentSystemResponse",
    )


class InstalledEquipmentStream(_BaseEquipmentSystemsStream, active_any=True):
    """Define installed equipment stream."""

    name = "installed_equipment"
    path = "/installed-equipment"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        EQUIPMENT_SYSTEMS,
        key="EquipmentSystems.V2.InstalledEquipmentResponse",
    )


class EquipmentTypesStream(_BaseEquipmentSystemsStream, active_any=True):
    """Define equipment types stream.

    https://developer.servicetitan.io/api-details/#api=tenant-equipment-systems-v2&operation=EquipmentTypes_GetList
    """

    name = "equipment_types"
    path = "/equipment-types"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(
        EQUIPMENT_SYSTEMS,
        key="EquipmentSystems.V2.EquipmentTypeResponse",
    )
