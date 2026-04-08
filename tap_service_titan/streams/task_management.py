"""Task management streams for the ServiceTitan tap."""

from __future__ import annotations

from tap_service_titan.client import ServiceTitanStream
from tap_service_titan.openapi_specs import TASK_MANAGEMENT, ServiceTitanSchema


class _BaseTaskManagementStream(ServiceTitanStream, api_prefix="/task-management/v2"):
    pass


class TasksStream(_BaseTaskManagementStream):
    """Define tasks stream."""

    name = "tasks"
    path = "/tasks"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(TASK_MANAGEMENT, key="TaskManagement.V2.TaskGetResponse")
