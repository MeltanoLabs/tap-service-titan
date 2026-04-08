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
1. Add the stream class to the appropriate `tap_service_titan/streams/<api>.py`
1. Register it in `tap.py` → `discover_streams()`

### Choosing the base class

Each API module defines per-API base classes that already embed the API prefix and the correct stream type. Inherit from one of those — **do not inherit from two parents**.

- **`_BaseXxxExportStream`** — for export endpoints (`/export/...`); wraps `ServiceTitanExportStream`; uses `continueFrom` pagination and `from` query param for incremental extraction
- **`_BaseXxxStream`** — for all other paginated endpoints; wraps `ServiceTitanStream`; uses page-number pagination and `modifiedOnOrAfter` for incremental

If the target API module doesn't have a matching base class yet, add one:

```python
class _BaseMyApiStream(ServiceTitanStream, api_prefix="/myapi/v2"):
    pass

class _BaseMyApiExportStream(ServiceTitanExportStream, api_prefix="/myapi/v2"):
    pass
```

### Schema

Prefer `ServiceTitanSchema(SPEC_CONSTANT, key="Component.Schema.Name")` over manually defining schemas with `th.PropertiesList`. The schema key comes from `components/schemas` in the OpenAPI spec.

```python
class MyStream(_BaseMyApiStream):
    name = "my_stream"
    path = "/my-endpoint"
    primary_keys = ("id",)
    replication_key: str = "modifiedOn"  # omit if no date field
    schema = ServiceTitanSchema(MYAPI, key="MyApi.V2.MyResponse")
```

### Class kwargs

Pass these as keyword arguments on the class definition line:

- `active_any=True` — adds `active=Any` query param to include inactive records
- `sort_by="fieldName"` — adds `sort=+fieldName` and marks stream as sorted
- `page_size=500` — overrides the default page size of 5000 (use when the endpoint has a lower cap)
- `include_total=True` — adds `includeTotal=True` query param (required by some endpoints)
- `first_page=0` — overrides the default first page number of 1 (use when pagination starts at 0)

Example combining several:

```python
class MyStream(_BaseMyApiStream, active_any=True, sort_by="ModifiedOn", page_size=500):
    ...
```
