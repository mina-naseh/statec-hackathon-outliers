from lux_comext.config import load_settings, match_patterns
from lux_comext.sources.catalogue import fetch_inventory


def test_load_settings():
    settings = load_settings()
    assert settings["project"]["name"] == "lux-comext"
    assert settings["filters"]["declarant"] == ["LU"]
    assert settings["downloads"]["sources"][0]["directory"] == "comext/COMEXT_DATA/PRODUCTS"


def test_match_patterns():
    assert (
        match_patterns("full_v2_202501.7z", include=["full_v2_*.7z"], exclude=["full_v2_*52.7z"])
        is True
    )
    assert (
        match_patterns("full_v2_202552.7z", include=["full_v2_*.7z"], exclude=["full_v2_*52.7z"])
        is False
    )
    assert (
        match_patterns(
            "full_partxiu_202501.7z", include=["full_v2_*.7z"], exclude=["full_partxiu_*.7z"]
        )
        is False
    )


def test_fetch_inventory_reads_html_listing(monkeypatch):
    html = """
    <html><body>
    <a href="?file=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull_v2_202501.7z">full_v2_202501.7z</a>
    <a href="/eurostat/api/dissemination/files?dir=comext%2FCOMEXT_DATA%2FPRODUCTS">parent</a>
    <a href="?file=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull_v2_202502.7z">full_v2_202502.7z</a>
    </body></html>
    """

    class DummyResponse:
        text = html
        url = "https://ec.europa.eu/eurostat/api/dissemination/files/"
        status_code = 200

        def raise_for_status(self):
            return None

    def fake_get(url, params=None, timeout=None):
        assert params["dir"] == "comext/COMEXT_DATA/PRODUCTS"
        assert params["sort"] == "1"
        return DummyResponse()

    monkeypatch.setattr("lux_comext.sources.catalogue.requests.get", fake_get)

    rows = fetch_inventory(
        "https://ec.europa.eu/eurostat/api/dissemination/files", "comext/COMEXT_DATA/PRODUCTS"
    )

    assert [row["name"] for row in rows] == [
        "comext/COMEXT_DATA/PRODUCTS/full_v2_202501.7z",
        "comext/COMEXT_DATA/PRODUCTS/full_v2_202502.7z",
    ]
    assert all(row["type"] == "file" for row in rows)
