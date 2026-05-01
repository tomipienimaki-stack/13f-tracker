import os
import time
import requests
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://data.sec.gov"
ARCHIVE_URL = "https://www.sec.gov/Archives/edgar/data"
_REQUEST_DELAY = 0.4  # ~2.5 req/s, well under 10/s limit


def _session() -> requests.Session:
    s = requests.Session()
    user_agent = os.getenv("EDGAR_USER_AGENT")
    if not user_agent:
        raise RuntimeError("EDGAR_USER_AGENT not set in .env")
    s.headers.update({"User-Agent": user_agent, "Accept-Encoding": "gzip, deflate"})
    return s


_s = _session()


def _get(url: str) -> requests.Response:
    time.sleep(_REQUEST_DELAY)
    r = _s.get(url, timeout=30)
    r.raise_for_status()
    return r


def get_filings(cik: str) -> list[dict]:
    """Return list of 13F-HR filings for the given CIK, newest first."""
    cik_padded = cik.zfill(10)
    data = _get(f"{BASE_URL}/submissions/CIK{cik_padded}.json").json()

    filings = []
    recent = data.get("filings", {}).get("recent", {})
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    periods = recent.get("reportDate", [])
    accessions = recent.get("accessionNumber", [])

    for form, date, period, accession in zip(forms, dates, periods, accessions):
        if form in ("13F-HR", "13F-HR/A"):
            filings.append({
                "form": form,
                "filing_date": date,
                "period": period,
                "accession": accession,
                "cik": cik_padded,
            })

    # Also check older filings pages if available
    if not filings:
        print(f"  [warn] No 13F-HR filings in recent batch for CIK {cik}")

    return filings  # already newest-first per EDGAR order


def get_infotable_xml(cik: str, accession: str) -> str:
    """Download the primary 13F INFOTABLE XML for an accession."""
    cik_num = str(int(cik))  # strip leading zeros for archive path
    acc_clean = accession.replace("-", "")
    index_url = f"{ARCHIVE_URL}/{cik_num}/{acc_clean}/{accession}-index.json"

    try:
        index = _get(index_url).json()
    except Exception:
        # Fallback: try without dashes in filename
        index_url2 = f"{ARCHIVE_URL}/{cik_num}/{acc_clean}/index.json"
        index = _get(index_url2).json()

    # Find the INFOTABLE document
    docs = index.get("directory", {}).get("item", [])
    infotable_name = None
    for doc in docs:
        name = doc.get("name", "").lower()
        if "infotable" in name and name.endswith(".xml"):
            infotable_name = doc["name"]
            break

    if not infotable_name:
        # Sometimes named differently — grab first XML that isn't the primary doc
        for doc in docs:
            name = doc.get("name", "").lower()
            if name.endswith(".xml") and "primary_doc" not in name:
                infotable_name = doc["name"]
                break

    if not infotable_name:
        raise ValueError(f"No INFOTABLE XML found in {index_url}")

    xml_url = f"{ARCHIVE_URL}/{cik_num}/{acc_clean}/{infotable_name}"
    return _get(xml_url).text
