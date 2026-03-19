# tap-service-titan

A Meltano Singer tap for the ServiceTitan API, built with the Singer SDK.

## Commands

```bash
uv run tap-service-titan --help
uv run pytest

# After adding/changing a stream schema, update snapshots:
uv run pytest --snapshot-update tests/test_schema_evolution.py
```

## Adding a Stream

1. Find the endpoint in `tap_service_titan/openapi_specs/<api>-v2.json`
2. Add the stream class to the appropriate `tap_service_titan/streams/<api>.py`
3. Register it in `tap.py` → `discover_streams()`

### Choosing the base class

- **`ServiceTitanExportStream`** — export endpoints (`/export/...`), supports `continueFrom` pagination and `from` query param for incremental extraction
- **`ServiceTitanStream`** — all other paginated endpoints; uses page-number pagination and `modifiedOnOrAfter` for incremental

### Schema

Prefer `ServiceTitanSchema(SPEC_CONSTANT, key="Component.Schema.Name")` over manually defining schemas with `th.PropertiesList`. The schema key comes from `components/schemas` in the OpenAPI spec.

```python
class MyStream(ServiceTitanStream):
    name = "my_stream"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"  # omit if no date field
    schema = ServiceTitanSchema(SETTINGS, key="Namespace.V2.MyResponse")

    @override
    @cached_property
    def path(self) -> str:
        return f"/settings/v2/tenant/{self.tenant_id}/my-endpoint"
```

### Path construction

The `url_base` is `config["api_url"]` (e.g. `https://api-integration.servicetitan.io`). The OpenAPI spec `servers[0].url` tells you the API prefix (e.g. `/settings/v2`). Combine them: `/settings/v2/tenant/{self.tenant_id}/...`.

### Class kwargs

- `active_any=True` — adds `active=Any` query param to include inactive records
- `sort_by="fieldName"` — adds `sort=+fieldName` and marks stream as sorted
