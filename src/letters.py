"""
Sijoittajakirjeet — lataa PDF, analysoi Claudella ja vertaa 13F-muutoksiin.

Käyttö:
    python -m src.letters --cik 0001067983
    python -m src.letters --cik 0001067983 --year 2024
    python -m src.letters --all
"""

import argparse
import base64
import io
import sys
from pathlib import Path

import requests
import yaml

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from src import analyzer, database


def _download(url: str) -> bytes:
    headers = {"User-Agent": "Mozilla/5.0 (compatible; 13f-tracker/1.0)"}
    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.content


def _analyze_pdf(pdf_bytes: bytes, investor_name: str, year: int) -> str:
    """Lähetä PDF Claudelle ja palauta analyysi."""
    pdf_b64 = base64.standard_b64encode(pdf_bytes).decode("utf-8")

    prompt = f"""Tämä on {investor_name}n vuosikirje {year}.

Analysoi kirje seuraavasti:

**1. Yhteenveto** (max 200 sanaa)
Tiivistä kirjeen pääviestit.

**2. Sijoitusfilosofia**
Mitä {investor_name} sanoo omasta strategiastaan ja periaatteistaan?

**3. Avainteesit**
Listaa 3–5 konkreettista sijoitusteesiä tai markkinanäkemystä kirjeestä.

**4. Riskit ja varoitukset**
Mitä riskejä tai huolenaiheita nostettiin esille?

Vastaa suomeksi. Perustu vain kirjeen sisältöön."""

    client = analyzer._get_client()
    response = client.beta.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        betas=["pdfs-2024-09-25"],
        system=[{
            "type": "text",
            "text": analyzer.SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"},
        }],
        messages=[{
            "role": "user",
            "content": [
                {
                    "type": "document",
                    "source": {
                        "type": "base64",
                        "media_type": "application/pdf",
                        "data": pdf_b64,
                    },
                },
                {"type": "text", "text": prompt},
            ],
        }],
    )
    usage = response.usage
    print(
        f"  [Claude] in={usage.input_tokens} out={usage.output_tokens} "
        f"cache_read={getattr(usage, 'cache_read_input_tokens', 0)}"
    )
    return response.content[0].text


def _compare_with_holdings(conn, cik: str, letter_analysis: str, investor_name: str) -> str:
    """Vertaa kirjeen teesejä viimeisimpiin 13F-muutoksiin."""
    rows = conn.execute("""
        SELECT c.company_name, c.change_type, c.value_delta
        FROM changes c
        WHERE c.cik = ?
        ORDER BY c.period_to DESC, ABS(c.value_delta) DESC
        LIMIT 30
    """, (cik,)).fetchall()

    if not rows:
        return "Ei 13F-dataa vertailuun — aja ensin tracker."

    def fmt(v):
        v = abs(v)
        if v >= 1_000_000_000:
            return f"${v/1e9:.1f}B"
        if v >= 1_000_000:
            return f"${v/1e6:.0f}M"
        return f"${v/1e3:.0f}K"

    holdings_text = "\n".join(
        f"  - {r['company_name']}: {r['change_type']} ({fmt(r['value_delta'])})"
        for r in rows
    )

    prompt = f"""Vertaa {investor_name}n kirjeessä ilmaistuja teesejä viimeisimpiin 13F-muutoksiin.

KIRJEEN ANALYYSI:
{letter_analysis}

VIIMEISIMMÄT 13F-MUUTOKSET:
{holdings_text}

Tehtävä:
1. Tukevatko 13F-muutokset kirjeen teesejä? Mitkä liikkeet sopivat yhteen, mitkä ristiriidassa? (max 200 sanaa)
2. Onko jotain, mitä kirjoitettiin mutta ei tehty — tai tehtiin ilman selitystä? (max 100 sanaa)

Vastaa suomeksi. Perustele datalla."""

    client = analyzer._get_client()
    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=[{
            "type": "text",
            "text": analyzer.SYSTEM_PROMPT,
            "cache_control": {"type": "ephemeral"},
        }],
        messages=[{"role": "user", "content": prompt}],
    )
    usage = response.usage
    print(
        f"  [Claude] in={usage.input_tokens} out={usage.output_tokens} "
        f"cache_read={getattr(usage, 'cache_read_input_tokens', 0)}"
    )
    return response.content[0].text


def process_letter(inv: dict, year: int) -> None:
    cik = inv["cik"]
    investor_name = inv["name"]
    letter_url = inv.get("letter_url", "")

    if not letter_url:
        print(f"  {investor_name}: letter_url puuttuu investors.yaml:sta.")
        return

    url = letter_url.format(year=year)
    print(f"\n  Ladataan {investor_name}n kirje {year}...")
    print(f"  URL: {url}")

    try:
        pdf_bytes = _download(url)
        print(f"  PDF: {len(pdf_bytes) // 1024} KB")
    except Exception as e:
        print(f"  VIRHE latauksessa: {e}")
        return

    print("  Analysoidaan Claudella...")
    analysis = _analyze_pdf(pdf_bytes, investor_name, year)

    conn = database.connect()
    database.init_schema(conn)
    database.upsert_investor(conn, cik, inv["name"], inv.get("company", ""), inv.get("strategy", ""))
    database.upsert_letter(conn, cik, str(year), url, analysis)

    print("\n" + "─" * 60)
    print(f"  KIRJEANALYYSI — {investor_name} {year}")
    print("─" * 60)
    print(analysis)
    print("─" * 60)

    print("\nVertaillaan 13F-muutoksiin...")
    comparison = _compare_with_holdings(conn, cik, analysis, investor_name)

    # Tallennetaan vertailu key_theses-kenttään
    database.upsert_letter(conn, cik, str(year), url, analysis, comparison)

    print("\n" + "─" * 60)
    print(f"  KIRJE vs 13F — {investor_name} {year}")
    print("─" * 60)
    print(comparison)
    print("─" * 60)
    print(f"\nTallennettu tietokantaan: {database.DB_PATH}")


def run_letters(cik: str | None, year: int | None, run_all: bool) -> None:
    config_path = Path(__file__).parent.parent / "config" / "investors.yaml"
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    investors = cfg.get("investors", [])

    if run_all:
        targets = [inv for inv in investors if inv.get("active") and inv.get("letter_url")]
    elif cik:
        targets = [inv for inv in investors if inv["cik"].lstrip("0") == cik.lstrip("0")]
    else:
        print("Anna --cik tai --all.")
        return

    if not targets:
        print("Ei sopivia sijoittajia (tarkista letter_url investors.yaml:ssa).")
        return

    import datetime
    default_year = datetime.date.today().year - 1

    for inv in targets:
        letter_years = inv.get("letter_years", [default_year])
        years = [year] if year else letter_years
        for y in years:
            process_letter(inv, y)


def main() -> None:
    ap = argparse.ArgumentParser(description="13F Letters — sijoittajakirjeiden analyysi")
    ap.add_argument("--cik", help="Sijoittajan CIK")
    ap.add_argument("--year", type=int, help="Kirjevuosi (oletus: viime vuosi)")
    ap.add_argument("--all", action="store_true", dest="run_all", help="Kaikki aktiiviset joilla letter_url")
    args = ap.parse_args()
    run_letters(cik=args.cik, year=args.year, run_all=args.run_all)


if __name__ == "__main__":
    main()
