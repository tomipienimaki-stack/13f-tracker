import re
import xml.etree.ElementTree as ET
from typing import Any


# EDGAR 13F XML uses these namespace variants
_NS_PATTERNS = [
    "{http://www.sec.gov/edgar/document/thirteenf/informationtable}",
    "{http://www.sec.gov/edgar/thirteenf/informationtable}",
    "",  # no namespace fallback
]


def _strip_ns(tag: str) -> str:
    return re.sub(r"\{[^}]*\}", "", tag)


def parse_infotable(xml_text: str) -> list[dict[str, Any]]:
    """
    Parse 13F INFOTABLE XML into a list of holding dicts.
    Returns: [{cusip, name_of_issuer, value_usd, shares, put_call}, ...]
    value_usd is in dollars (EDGAR stores as dollars despite spec saying thousands).
    """
    root = ET.fromstring(xml_text)

    holdings = []
    for info in root.iter():
        if _strip_ns(info.tag) == "infoTable":
            h = _parse_info_table_entry(info)
            if h:
                holdings.append(h)

    return holdings


def _parse_info_table_entry(node) -> dict | None:
    data = {_strip_ns(child.tag): (child.text or "").strip() for child in node.iter()}

    try:
        # Modern EDGAR 13F XML stores value in actual dollars (not thousands)
        # despite the spec saying "thousands" — Berkshire's filings confirm this.
        value_thousands = int(data.get("value", "0").replace(",", "") or 0)
        shares_raw = (
            data.get("sshPrnamt") or data.get("shrsorprnamt") or "0"
        ).replace(",", "")
        shares = int(shares_raw or 0)
    except ValueError:
        return None

    return {
        "cusip": data.get("cusip", ""),
        "name_of_issuer": data.get("nameOfIssuer", data.get("nameofissuer", "")),
        "value_usd": value_thousands,
        "shares": shares,
        "put_call": data.get("putCall", data.get("putcall", "")),
        "investment_discretion": data.get(
            "investmentDiscretion", data.get("investmentdiscretion", "")
        ),
    }
