from lux_comext.parsing.reader import normalize_record


def test_normalize_record_preserves_strings_and_missing_values():
    row = {
        "REPORTER": "LU",
        "PARTNER": "DE",
        "PRODUCT_NC": "01012100",
        "FLOW": "1",
        "PERIOD": "202501",
        "VALUE_IN_EUROS": "123456",
        "QUANTITY_IN_KG": "",
        "SOURCE_ARCHIVE": "full_v2_202501.7z",
    }

    result = normalize_record(row)
    assert result["declarant"] == "LU"
    assert result["partner"] == "DE"
    assert result["product_nc"] == "01012100"
    assert result["flow"] == 1
    assert result["period"] == 202501
    assert result["value_in_euros"] == 123456
    assert result["quantity_in_kg"] is None
    assert result["source_archive"] == "full_v2_202501.7z"
