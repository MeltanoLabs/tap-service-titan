"""Scrape the ServiceTitan OpenAPI specs."""  # noqa: INP001

import asyncio
import json
from collections.abc import Generator

import anyio
import rich
from bs4 import BeautifulSoup
from playwright.async_api import Playwright, async_playwright

DOCS_ROOT = "https://developer.servicetitan.io"
APIS_URL = f"{DOCS_ROOT}/docs/apis"
OUTPUT_DIR = anyio.Path("tap_service_titan/openapi_specs")


def get_api_name_from_url(url: str) -> str:
    """Get the API name from a URL."""
    return url.rstrip("/").rsplit("/", maxsplit=1)[-1].replace("tenant-", "")


async def get_soup_from_url(playwright: Playwright, url: str) -> BeautifulSoup:
    """Get a BeautifulSoup object from a URL."""
    rich.print(f"Getting soup from {url}")
    browser = await playwright.chromium.launch()
    page = await browser.new_page()
    page.set_default_timeout(100000)
    await page.goto(url, wait_until="networkidle")
    html = await page.content()
    await browser.close()
    return BeautifulSoup(html, "html.parser")


async def download_openapi_spec(
    playwright: Playwright,
    url: str,
    download_path: anyio.Path,
) -> None:
    """Download an OpenAPI spec from a URL."""
    rich.print(f"Downloading OpenAPI spec from {url} → {download_path}")
    browser = await playwright.chromium.launch()
    page = await browser.new_page()
    page.set_default_timeout(100000)
    await page.goto(url, wait_until="networkidle")
    async with page.expect_download() as download_info:
        await page.get_by_text("OpenAPI Document", exact=False).click()
    download = await download_info.value

    async with anyio.NamedTemporaryFile(mode="wb+", suffix=".json", delete=True) as tmp_file:
        await download.save_as(tmp_file.name)  # ty:ignore[invalid-argument-type]
        await tmp_file.seek(0)
        data = json.loads(await tmp_file.read())

    async with await anyio.open_file(download_path, mode="w", encoding="utf-8") as f:
        # Preserve the current indentation
        await f.write(json.dumps(data, indent=4))

    await browser.close()


def _extract_links(soup: BeautifulSoup) -> Generator[str, None, None]:
    """Extract all API details page links from the root BeautifulSoup object."""
    for a in soup.find_all("a"):
        href = a.get("href")
        if href and isinstance(href, str) and "/docs/apis/" in href:
            yield f"{DOCS_ROOT}{href}" if href.startswith("/") else href


async def download_all_openapi_specs() -> None:
    """Download all ServiceTitan OpenAPI specs."""
    async with async_playwright() as playwright:
        soup = await get_soup_from_url(playwright, APIS_URL)
        await OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        tasks = []
        for api_url in _extract_links(soup):
            api_name = get_api_name_from_url(api_url)
            download_path = OUTPUT_DIR / f"{api_name}.json"
            rich.print(f"Queuing {api_url} → {download_path}")
            tasks.append(download_openapi_spec(playwright, api_url, download_path))
        await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(download_all_openapi_specs())
