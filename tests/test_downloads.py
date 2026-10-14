from unittest.mock import Mock

import pytest
import requests
from typer.testing import CliRunner

from lux_comext.cli import app
from lux_comext.sources.catalogue import fetch_inventory
from lux_comext.sources.downloader import download_sources, validate_download_url

LISTING = "https://ec.europa.eu/eurostat/api/dissemination/files/"
DIRECTORY = "comext/COMEXT_DATA/PRODUCTS"
FILE = f"{DIRECTORY}/full_v2_202501.7z"
LINK = f"{LISTING}?sort=1&downfile=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull_v2_202501.7z"
HTML = """
<a href="?sort=1&amp;dir=comext%2FCOMEXT_DATA%2FPRODUCTS">Name</a>
<a href="?sort=1&amp;file=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull_v2_202501.7z">file</a>
<a href="?sort=1&amp;downfile=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull_v2_202501.7z">Download</a>
"""


def response(status=200, content_type="application/octet-stream", text="", url=LISTING):
    result = requests.Response()
    result.status_code = status
    result.url = url
    result.headers["Content-Type"] = content_type
    result._content = text.encode()
    result._content_consumed = True
    return result


def settings(tmp_path):
    return {
        "eurostat": {"files_api": LISTING.rstrip("/")},
        "downloads": {
            "sources": [
                {
                    "name": "monthly_trade",
                    "enabled": True,
                    "directory": DIRECTORY,
                    "include": ["full_v2_*.7z"],
                }
            ]
        },
        "storage": {"raw_dir": str(tmp_path / "raw")},
    }


def test_dry_run_refreshes_listing_and_checks_published_links_every_time(monkeypatch, tmp_path):
    listing = Mock(
        side_effect=[
            response(text=HTML, content_type="text/html"),
            response(text=HTML.replace("202501", "202502"), content_type="text/html"),
        ]
    )
    head = Mock(side_effect=lambda *args, **kwargs: response())
    monkeypatch.setattr("lux_comext.sources.catalogue.requests.get", listing)
    monkeypatch.setattr("lux_comext.sources.downloader.requests.head", head)
    config = settings(tmp_path)
    # Even a locally existing archive must be checked on a dry run.
    local_file = tmp_path / "raw" / "monthly_trade" / "full_v2_202501.7z"
    local_file.parent.mkdir(parents=True)
    local_file.write_bytes(b"existing")

    first = download_sources(config, dry_run=True)
    second = download_sources(config, dry_run=True)

    assert first == [{"source_name": "monthly_trade", "name": FILE, "url": LINK}]
    assert second[0]["url"] == LINK.replace("202501", "202502")
    assert listing.call_count == 2
    listing.assert_called_with(LISTING, params={"sort": "1", "dir": DIRECTORY}, timeout=60)
    assert [call.args[0] for call in head.call_args_list] == [LINK, second[0]["url"]]
    assert local_file.read_bytes() == b"existing"
    assert list((tmp_path / "raw").rglob("*.part")) == []


@pytest.mark.parametrize(
    "status,content_type,error",
    [
        (404, "application/octet-stream", requests.HTTPError),
        (500, "text/html", requests.HTTPError),
        (200, "text/html; charset=utf-8", ValueError),
        (200, "application/json", ValueError),
        (204, "application/octet-stream", ValueError),
    ],
)
def test_validation_rejects_invalid_urls(monkeypatch, status, content_type, error):
    monkeypatch.setattr(requests, "head", lambda *a, **kw: response(status, content_type))
    with pytest.raises(error):
        validate_download_url(LINK)


@pytest.mark.parametrize("status", [405, 501])
def test_validation_falls_back_to_streamed_get(monkeypatch, status):
    monkeypatch.setattr(requests, "head", lambda *a, **kw: response(status))
    get = Mock(return_value=response())
    monkeypatch.setattr(requests, "get", get)
    validate_download_url(LINK)
    get.assert_called_once_with(LINK, stream=True, timeout=60)


def test_dry_run_checks_all_urls_without_creating_files(monkeypatch, tmp_path):
    monkeypatch.setattr(
        requests,
        "get",
        lambda *a, **kw: response(
            text=HTML + HTML.replace("202501", "202502"), content_type="text/html"
        ),
    )
    head = Mock(side_effect=lambda *a, **kw: response())
    monkeypatch.setattr(requests, "head", head)
    targets = download_sources(settings(tmp_path), dry_run=True)
    assert len(targets) == 2
    assert [call.args[0] for call in head.call_args_list] == [t["url"] for t in targets]
    assert not (tmp_path / "raw").exists()


def test_streamed_get_fallback_also_rejects_errors(monkeypatch):
    monkeypatch.setattr(requests, "head", lambda *a, **kw: response(405))
    monkeypatch.setattr(requests, "get", lambda *a, **kw: response(404))
    with pytest.raises(requests.HTTPError):
        validate_download_url(LINK)


def test_listing_failure_does_not_become_empty_success(monkeypatch, tmp_path):
    monkeypatch.setattr(requests, "get", Mock(side_effect=requests.Timeout("listing timed out")))
    with pytest.raises(requests.Timeout):
        download_sources(settings(tmp_path), dry_run=True)
    assert not (tmp_path / "raw").exists()


def test_html_error_page_is_not_an_empty_inventory(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **kw: response(text="<html>Error</html>"))
    with pytest.raises(ValueError, match="No file links"):
        fetch_inventory(LISTING, DIRECTORY)


def test_json_error_page_is_not_an_empty_inventory(monkeypatch):
    monkeypatch.setattr(requests, "get", lambda *a, **kw: response(text='{"error": "failed"}'))
    with pytest.raises(ValueError, match="Unrecognized Eurostat listing"):
        fetch_inventory(LISTING, DIRECTORY)


def test_csv_inventory_qualifies_bare_filenames(monkeypatch):
    monkeypatch.setattr(
        requests,
        "get",
        lambda *a, **kw: response(
            text="NAME,TYPE\nfull_v2_202501.7z,file\n", content_type="text/csv"
        ),
    )
    rows = fetch_inventory(LISTING, DIRECTORY)
    assert rows[0]["name"] == FILE
    assert "file=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull_v2_202501.7z" in rows[0]["url"]


def test_cli_dry_run_exits_nonzero_without_printing_unvalidated_plan(monkeypatch):
    monkeypatch.setattr("lux_comext.cli.setup_logging", Mock())
    monkeypatch.setattr(
        "lux_comext.cli.download_sources", Mock(side_effect=ValueError("Invalid download URL"))
    )
    result = CliRunner().invoke(app, ["download", "--dry-run"])
    assert result.exit_code == 1
    assert "Download check failed" in result.output
    assert "Validated downloads" not in result.output
