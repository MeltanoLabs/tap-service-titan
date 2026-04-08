"""Forms streams for the ServiceTitan tap."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from singer_sdk import typing as th  # JSON Schema typing helpers

from tap_service_titan.client import ServiceTitanStream
from tap_service_titan.openapi_specs import FORMS, ServiceTitanSchema
from tap_service_titan.streams.jpm import JobsStream

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if TYPE_CHECKING:
    from collections.abc import Iterable

    from singer_sdk.helpers.types import Context


class _BaseFormsStream(ServiceTitanStream, api_prefix="/forms/v2"):
    pass


class FormsStream(_BaseFormsStream):
    """Define forms stream."""

    name = "forms"
    path = "/forms"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"
    schema = ServiceTitanSchema(FORMS, key="Forms.V2.FormResponse")


class SubmissionsStream(_BaseFormsStream):
    """Define submissions stream."""

    name = "submissions"
    path = "/submissions"
    primary_keys = ("id",)
    replication_key: str = "submittedOn"
    is_sorted = False

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("formId", th.IntegerType),
        th.Property("formName", th.StringType),
        th.Property("submittedOn", th.DateTimeType),
        th.Property("createdById", th.IntegerType),
        th.Property("status", th.StringType),
        th.Property(
            "owners",
            th.ArrayType(
                th.ObjectType(
                    th.Property("type", th.StringType),
                    th.Property("id", th.IntegerType),
                )
            ),
        ),
        th.Property(
            "units",
            th.ArrayType(
                th.ObjectType(
                    th.Property("id", th.StringType),
                    th.Property("name", th.StringType),
                    th.Property("type", th.StringType),
                    th.Property("description", th.StringType),
                    th.Property(
                        "units",
                        th.ArrayType(
                            th.ObjectType(
                                th.Property("id", th.StringType),
                                th.Property("name", th.StringType),
                                th.Property("type", th.StringType),
                                th.Property("comment", th.StringType),
                                th.Property(
                                    "attachments",
                                    th.ArrayType(
                                        th.ObjectType(
                                            th.Property("fileName", th.StringType),
                                            th.Property("createdFrom", th.StringType),
                                            th.Property("originalFileName", th.StringType),
                                            th.Property("thumbnail", th.StringType),
                                        )
                                    ),
                                ),
                                th.Property("value", th.StringType),
                                th.Property("options", th.StringType),
                                th.Property(
                                    "values",
                                    th.ArrayType(th.CustomType({"type": ["string", "null"]})),
                                ),
                                th.Property("isRefused", th.BooleanType),
                                th.Property("refusalReason", th.StringType),
                                additional_properties=True,
                            )
                        ),
                    ),
                    additional_properties=True,
                )
            ),
        ),
    ).to_dict()

    @override
    def get_records(self, context: Context | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects with coerced values.

        Args:
            context: Stream partition or context dictionary.

        Yields:
            One item per record with string coercion only in the units section.
        """

        def coerce_value(value: Any) -> str | None:  # noqa: ANN401
            """Convert value to string if it has content, else None."""
            if value is None or value == "":
                return None
            return str(value)

        def process_units(units_data: dict[str, Any]) -> dict[str, Any]:
            """Recursively process units dictionary values."""
            result: dict[str, Any] = {}
            for key, value in units_data.items():
                if isinstance(value, dict):
                    result[key] = process_units(value)
                elif isinstance(value, list):
                    result[key] = [
                        process_units(item) if isinstance(item, dict) else coerce_value(item)
                        for item in value
                    ]
                else:
                    result[key] = coerce_value(value)
            return result

        # Get records from parent class implementation
        parent_records = super().get_records(context)

        # Process each record
        for record in parent_records:
            if "units" in record:
                # Only process the units section, leaving the rest of the record unchanged
                record["units"] = [process_units(unit) for unit in record["units"]]
            yield record

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, Any]:
        """Overrides the default URL parameters to use a custom param for incremental extraction.

        The `modifiedOnOrAfter` param is not supported by this endpoint, so we're using
        `submittedOnOrAfter` instead.
        """
        params: dict[str, Any] = {}
        if self.replication_key and (starting_date := self.get_starting_timestamp(context)):
            # isoformat includes timezone +00:00 etc which is not supported by the API,
            # this format is better
            params["submittedOnOrAfter"] = starting_date.strftime("%Y-%m-%dT%H:%M:%SZ")

        # We were getting Memory Issues with page size of 5000 so we're using a custom page size.
        params["pageSize"] = 500
        params["page"] = next_page_token
        params["sort"] = "+SubmittedOn"  # Sort by submittedOn in ascending order
        return params


class JobAttachmentsStream(_BaseFormsStream):
    """Define forms stream."""

    name = "job_attachments"
    path = "/jobs/{job_id}/attachments"
    primary_keys = ("id",)
    replication_key: str = "createdOn"
    parent_stream_type = JobsStream

    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("job_id", th.IntegerType),
        th.Property("fileName", th.StringType),
        th.Property("createdById", th.IntegerType),
        th.Property("createdOn", th.DateTimeType),
    ).to_dict()
