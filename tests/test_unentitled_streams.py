"""Tests for skipping streams the tenant is not entitled to."""

from __future__ import annotations

import json
from http import HTTPStatus

import pytest
import requests
from singer_sdk.exceptions import FatalAPIError

from tap_service_titan.streams.pricebook import DiscountsAndFeesStream
from tap_service_titan.tap import TapServiceTitan

BASE_CONFIG = {
    "client_id": "id",
    "client_secret": "secret",
    "st_app_key": "key",
    "tenant_id": "123456789",
    "api_url": "https://api.servicetitan.io",
    "auth_url": "https://auth.servicetitan.io/connect/token",
}


def _forbidden() -> requests.Response:
    """Build the 403 ServiceTitan returns for an unlicensed module."""
    response = requests.Response()
    response.status_code = HTTPStatus.FORBIDDEN
    response.url = "https://api.servicetitan.io/pricebook/v2/tenant/123456789/discounts-and-fees"
    response._content = json.dumps(  # noqa: SLF001
        {"title": "Tenant is not authorized to access this resource."}
    ).encode()
    return response


def _stream(*, skip: bool) -> DiscountsAndFeesStream:
    tap = TapServiceTitan(
        config={**BASE_CONFIG, "skip_unentitled_streams": skip},
        validate_config=False,
    )
    stream = DiscountsAndFeesStream(tap)
    # Bypass the OAuth round-trip; these tests never reach a real endpoint.
    stream.__dict__["authenticator"] = None
    return stream


def _request_raising_403(stream: DiscountsAndFeesStream) -> None:
    """Make the stream's HTTP call surface a 403 the way a real one would."""

    def _raise(*_args: object, **_kwargs: object) -> None:
        stream.validate_response(_forbidden())

    stream._request = _raise  # type: ignore[method-assign]  # noqa: SLF001


def test_403_is_fatal_by_default() -> None:
    """Without the flag, a 403 still fails the run loudly."""
    with pytest.raises(FatalAPIError):
        _stream(skip=False).validate_response(_forbidden())


def test_403_skips_stream_when_enabled(caplog: pytest.LogCaptureFixture) -> None:
    """With the flag, the stream yields nothing and logs a warning."""
    stream = _stream(skip=True)
    _request_raising_403(stream)

    with caplog.at_level("WARNING"):
        assert list(stream.request_records(None)) == []

    assert "not entitled" in caplog.text
    assert stream.name in caplog.text


def test_403_leaves_bookmark_untouched() -> None:
    """A skipped stream must not advance state."""
    stream = _stream(skip=True)
    before = dict(stream.stream_state)
    _request_raising_403(stream)
    list(stream.request_records(None))

    assert dict(stream.stream_state) == before


@pytest.mark.parametrize("status", [HTTPStatus.UNAUTHORIZED, HTTPStatus.NOT_FOUND])
def test_other_4xx_still_fatal(status: HTTPStatus) -> None:
    """Only 403 is treated as an entitlement signal; 401 is auth, not licensing."""
    response = requests.Response()
    response.status_code = status
    response.url = "https://api.servicetitan.io/pricebook/v2/tenant/123456789/discounts-and-fees"
    response._content = b"{}"  # noqa: SLF001

    with pytest.raises(FatalAPIError):
        _stream(skip=True).validate_response(response)
