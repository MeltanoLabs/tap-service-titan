"""Tools for exploring OpenAPI specs."""  # noqa: INP001

import json
from pathlib import Path
from typing import Annotated

import jsonref
import typer

app = typer.Typer()


def get_response_spec_from_path(full_spec, path):
    """Get the response schema from a path."""
    return full_spec["paths"][path]["get"]["responses"]["200"]["content"]["application/json"][
        "schema"
    ]["properties"]["data"]["items"]


def get_spec_from_path(path: Path):  # noqa: D103
    with path.open("r") as spec_file:
        return jsonref.replace_refs(json.load(spec_file))


def _get_paths_with_get(full_spec: dict):
    """Get all paths with a GET method."""
    for path in full_spec["paths"]:
        if "get" in full_spec["paths"][path]:
            yield path


@app.command()
def get_paths_with_get(
    path: Annotated[
        Path,
        typer.Option(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
    ],
):
    """Get all paths."""
    full_spec = get_spec_from_path(path)
    for path in _get_paths_with_get(full_spec):  # noqa: PLR1704
        print(path)


@app.command()
def get_export_specs(
    path: Annotated[
        Path,
        typer.Option(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
        ),
    ],
):
    """Get export specs."""
    full_spec = get_spec_from_path(path)
    for path in _get_paths_with_get(full_spec):  # noqa: PLR1704
        if "/export/" in path:
            path_spec = full_spec["paths"][path]
            items_schema = path_spec["get"]["responses"]["200"]["content"]["application/json"][
                "schema"
            ]["properties"]["data"]["items"]
            print(path)
            print(items_schema)


def _get_response_spec_for_path(spec_path: Path, url_path: str):
    full_spec = get_spec_from_path(spec_path)
    return get_response_spec_from_path(full_spec, url_path)


@app.command()
def get_response_spec_for_path(
    spec_path: Annotated[
        Path, typer.Option(exists=True, file_okay=True, dir_okay=False, readable=True)
    ],
    url_path: str,
):
    """Get the response schema for a path."""
    print(_get_response_spec_for_path(spec_path, url_path))


if __name__ == "__main__":
    app()
