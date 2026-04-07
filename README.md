# `tap-service-titan`

ServiceTitan tap class.

Built with the [Meltano Singer SDK](https://sdk.meltano.com).

## Capabilities

- `catalog`
- `state`
- `discover`
- `about`
- `stream-maps`
- `schema-flattening`
- `batch`

## Supported Python Versions

- 3.10
- 3.11
- 3.12
- 3.13
- 3.14

## Settings

| Setting | Required | Default | Description |
|:--------|:--------:|:-------:|:------------|
| client_id | True | None | The client ID to use in authenticating. |
| client_secret | True | None | The client secret to use in authenticating. |
| st_app_key | True | None | The app key for the Service Titan app used to authenticate. |
| tenant_id | True | None | Tenant ID to pull records for. |
| api_url | False | https://api-integration.servicetitan.io | The url for the ServiceTitan API |
| auth_url | False | https://auth-integration.servicetitan.io/connect/token | The url for the ServiceTitan OAuth API |
| start_date | False | 2024-01-01T00:00:00Z | The start date for the records to pull. |
| custom_reports | False | None | Custom reports to extract. |
| stream_maps | False | None | Config object for stream maps capability. For more information check out [Stream Maps](https://sdk.meltano.com/en/latest/stream_maps.html). |
| stream_map_config | False | None | User-defined config values to be used within map expressions. |
| faker_config | False | None | Config for the [`Faker`](https://faker.readthedocs.io/en/master/) instance variable `fake` used within map expressions. Only applicable if the plugin specifies `faker` as an addtional dependency (through the `singer-sdk` `faker` extra or directly). |
| faker_config.seed | False | None | Value to seed the Faker generator for deterministic output: https://faker.readthedocs.io/en/master/#seeding-the-generator |
| faker_config.locale | False | None | One or more LCID locale strings to produce localized output for: https://faker.readthedocs.io/en/master/#localization |
| flattening_enabled | False | None | 'True' to enable schema flattening and automatically expand nested properties. |
| flattening_max_depth | False | None | The max depth to flatten schemas. |
| batch_config | False | None | |
| batch_config.encoding | False | None | Specifies the format and compression of the batch files. |
| batch_config.encoding.format | False | None | Format to use for batch files. |
| batch_config.encoding.compression | False | None | Compression format to use for batch files. |
| batch_config.storage | False | None | Defines the storage layer to use when writing batch files |
| batch_config.storage.root | False | None | Root path to use when writing batch files. |
| batch_config.storage.prefix | False | None | Prefix to use when writing batch files. |

A full list of supported settings and capabilities is available by running: `tap-service-titan --about`

### Custom Reports

An example of the input for the custom reports array config is below.
The values can be found in the Service Titan UI when the custom report is created.

```json
  "custom_reports": [
      {
          "report_category": "accounting",
          "report_name": "my_custom_accounting_report",
          "report_id": "123",
          "parameters": [
              {
                  "name": "AsOfDate",
                  "value": "2024-09-01"
              }
          ]
      }
  ]
```

Optionally add a `backfill_date_parameter` entry in the custom report item that references a date parameter which should be used to retrieve historical reports.
The parameter will be used to iterate that date value and extract the report for every day until the current date.
The backfill_date_parameter value must be in the parameters list.

```json
  "custom_reports": [
      {
          "report_category": "accounting",
          "report_name": "my_custom_accounting_report",
          "report_id": "123",
          "backfill_date_parameter": "AsOfDate",
          "parameters": [
              {
                  "name": "AsOfDate",
                  "value": "2024-09-01"
              }
          ]
      }
  ]
```

Optionally add `lookback_window_days` to define the amount of days to lookback when running incrementally.
This is used to handled retroactively updated data in previously synced reports.

```json
  "custom_reports": [
      {
          "report_category": "accounting",
          "report_name": "my_custom_accounting_report",
          "report_id": "123",
          "backfill_date_parameter": "AsOfDate",
          "lookback_window_days": 30,
          "parameters": [
              {
                  "name": "AsOfDate",
                  "value": "2024-09-01"
              }
          ]
      }
  ]
```

### Configure using environment variables

This Singer tap will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

## Usage

You can easily run `tap-service-titan` by itself or in a pipeline using [Meltano](https://meltano.com/).

### Executing the Tap Directly

```bash
tap-service-titan --version
tap-service-titan --help
tap-service-titan --config CONFIG --discover > ./catalog.json
```

## Developer Resources

Follow these instructions to contribute to this project.

### Initialize your Development Environment

Install `uv` following the [installation guide](https://docs.astral.sh/uv/getting-started/installation/), then:

```bash
uv sync
```

### Create and Run Tests

Create tests within the `tests` subfolder and
then run:

```bash
uv run pytest
```

You can also test the `tap-service-titan` CLI interface directly using `uv run`:

```bash
uv run tap-service-titan --help
```

#### Snapshot Testing

This project uses [Syrupy](https://github.com/tophat/syrupy) for snapshot testing to detect schema evolution. Snapshots are stored in `tests/__snapshots__/` as `.ambr` (Amber) files.

**To update snapshots** after intentional schema changes:

```bash
uv run pytest tests/test_schema_evolution.py --snapshot-update
```

### Testing with [Meltano](https://www.meltano.com)

_**Note:** This tap will work in any Singer environment and does not require Meltano.
Examples here are for convenience and to streamline end-to-end orchestration scenarios._

Next, install Meltano (if you haven't already) and any needed plugins:

```bash
# Install meltano
pipx install meltano
# Initialize meltano within this directory
cd tap-service-titan
meltano install
```

Now you can test and orchestrate using Meltano:

```bash
# Test invocation:
meltano invoke tap-service-titan --version
# OR run a test `elt` pipeline:
meltano elt tap-service-titan target-jsonl
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own taps and targets.
