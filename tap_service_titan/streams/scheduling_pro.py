"""Scheduling streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING

from tap_service_titan.client import ServiceTitanStream
from tap_service_titan.openapi_specs import SCHEDULING_PRO, ServiceTitanSchema

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from singer_sdk.helpers.types import Context, Record


class _BaseSchedulingProStream(ServiceTitanStream, api_prefix="/schedulingpro/v2"):
    pass


class SchedulersStream(_BaseSchedulingProStream):
    """Define schedulers stream."""

    name = "schedulers"
    path = "/schedulers"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(SCHEDULING_PRO, key="SchedulingPro.V2.SchedulerResponse")

    @override
    def get_child_context(self, record: Record, context: Context | None) -> Context:
        """Return a context dictionary for child streams."""
        return {"scheduler_id": record.get("id")}


class SchedulerSessionsStream(_BaseSchedulingProStream):
    """Define scheduler sessions stream."""

    name = "scheduler_sessions"
    path = "/schedulers/{scheduler_id}/sessions"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    parent_stream_type = SchedulersStream
    schema = ServiceTitanSchema(
        SCHEDULING_PRO,
        key="SchedulingPro.V2.SchedulerSessionResponse",
    )


class SchedulerPerformanceStream(_BaseSchedulingProStream):
    """Define scheduler performance stream."""

    name = "scheduler_performance"
    path = "/schedulers/{scheduler_id}/performance"
    primary_keys = ("id",)
    parent_stream_type = SchedulersStream
    ignore_parent_replication_key = True
    schema = ServiceTitanSchema(
        SCHEDULING_PRO,
        key="SchedulingPro.V2.SchedulerPerformanceResponse",
    )
