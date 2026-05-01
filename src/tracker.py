"""
13F Tracker — MVP
Hae, parsi, tallenna ja vertaa 13F-raportit + Claude-analyysi.

Käyttö:
    python -m src.tracker --cik 0001067983
    python -m src.tracker --cik 0001067983 --skip-ai
    python -m src.tracker --all
"""

import argparse
import sys
import io
import datetime
from pathlib import Path

# Force UTF-8 output on Windows
if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import yaml

from src import edgar_client, parser, database, analyzer, notifier


def _fmt_value(v: int) -> str:
    if v >= 1_000_000_000:
        return f"${v/1e9:.1f}B"
    if v >= 1_000_000:
        return f"${v/1e6:.1f}M"
    return f"${v/1e3:.0f}K"


def _fmt_delta(d: int) -> str:
    sign = "+" if d >= 0 else ""
    return f"{sign}{_fmt_value(abs(d)) if abs(d) > 0 else '$0'}"


def compute_changes(
    cik: str,
    period_from: str,
    period_to: str,
    holdings_old: list,
    holdings_new: list,
) -> list[dict]:
    old = {row["cusip"]: row for row in holdings_old}
    new = {row["cusip"]: row for row in holdings_new}

    changes = []

    for cusip, h in new.items():
        name = h["company_name"] or cusip
        if cusip not in old:
            changes.append({
                "cik": cik, "period_from": period_from, "period_to": period_to,
                "ticker": h["ticker"], "cusip": cusip, "company_name": name,
                "change_type": "NEW",
                "shares_delta": h["shares"],
                "value_delta": h["value_usd"],
            })
        else:
            o = old[cusip]
            pct_change = (h["shares"] - o["shares"]) / max(o["shares"], 1) * 100
            if abs(pct_change) >= 10:
                changes.append({
                    "cik": cik, "period_from": period_from, "period_to": period_to,
                    "ticker": h["ticker"] or o["ticker"], "cusip": cusip,
                    "company_name": name,
                    "change_type": "INCREASE" if pct_change > 0 else "DECREASE",
                    "shares_delta": h["shares"] - o["shares"],
                    "value_delta": h["value_usd"] - o["value_usd"],
                })

    for cusip, o in old.items():
        if cusip not in new:
            changes.append({
                "cik": cik, "period_from": period_from, "period_to": period_to,
                "ticker": o["ticker"], "cusip": cusip,
                "company_name": o["company_name"] or cusip,
                "change_type": "EXIT",
                "shares_delta": -o["shares"],
                "value_delta": -o["value_usd"],
            })

    return changes


def print_changes(investor_name: str, period_from: str, period_to: str, changes: list[dict]) -> None:
    groups = {"NEW": [], "INCREASE": [], "DECREASE": [], "EXIT": []}
    for c in changes:
        groups[c["change_type"]].append(c)

    labels = {
        "NEW": "UUDET POSITIOT",
        "INCREASE": "LISÄYKSET (>10%)",
        "DECREASE": "VÄHENNYKSET (>10%)",
        "EXIT": "EXITIT",
    }

    print(f"\n{'='*60}")
    print(f"  {investor_name.upper()}")
    print(f"  {period_from} → {period_to}")
    print(f"{'='*60}")

    any_change = False
    for change_type, label in labels.items():
        rows = sorted(groups[change_type], key=lambda x: abs(x["value_delta"]), reverse=True)
        if not rows:
            continue
        any_change = True
        print(f"\n{label}:")
        for r in rows:
            name = r["company_name"][:35].ljust(35)
            delta = _fmt_delta(r["value_delta"])
            if change_type in ("NEW", "EXIT"):
                val = _fmt_value(abs(r["value_delta"]))
                print(f"  {name}  {val:>10}")
            else:
                sign = "+" if r["value_delta"] >= 0 else "-"
                abs_delta = _fmt_value(abs(r["value_delta"]))
                print(f"  {name}  {sign}{abs_delta:>10}")

    if not any_change:
        print("\n  Ei merkittäviä muutoksia (kynnys: 10% omistusmuutos).")

    print()


def save_ai_analysis(conn, cik: str, period_from: str, period_to: str, analysis: str) -> None:
    """Store analysis text back into all changes rows for this period pair."""
    conn.execute(
        """UPDATE changes SET ai_analysis=?
           WHERE cik=? AND period_from=? AND period_to=?""",
        (analysis, cik, period_from, period_to),
    )
    conn.commit()


def run_tracker(cik: str, skip_ai: bool = False, notify: bool = True) -> dict | None:
    conn = database.connect()
    database.init_schema(conn)

    # Load investor info from config
    config_path = Path(__file__).parent.parent / "config" / "investors.yaml"
    investor_name = cik
    strategy = ""
    if config_path.exists():
        with open(config_path) as f:
            cfg = yaml.safe_load(f)
        for inv in cfg.get("investors", []):
            if inv["cik"].lstrip("0") == cik.lstrip("0"):
                investor_name = inv["name"]
                strategy = inv.get("strategy", "")
                database.upsert_investor(conn, inv["cik"], inv["name"], inv.get("company", ""), strategy)
                cik = inv["cik"]
                break

    print(f"\nHaetaan 13F-raportit: {investor_name} (CIK {cik})")

    filings = edgar_client.get_filings(cik)
    if len(filings) < 2:
        print(f"  Ohitetaan: alle 2 13F-raporttia ({len(filings)} kpl).")
        return None

    # Process two latest filings
    for filing in filings[:2]:
        period = filing["period"]
        accession = filing["accession"]
        print(f"  Ladataan {period} ({accession})...")

        xml_text = edgar_client.get_infotable_xml(cik, accession)
        holdings = parser.parse_infotable(xml_text)
        print(f"    Löydettiin {len(holdings)} omistusta.")

        filing_id = database.upsert_filing(conn, cik, filing["filing_date"], period, accession)
        database.insert_holdings(conn, filing_id, holdings)

    # Compare the two
    db_filings = database.get_two_latest_filings(conn, cik)
    if len(db_filings) < 2:
        print("Ei kahta raporttia vertailtavaksi vielä.")
        return None

    newer, older = db_filings[0], db_filings[1]
    holdings_new = database.get_holdings_for_filing(conn, newer["id"])
    holdings_old = database.get_holdings_for_filing(conn, older["id"])

    changes = compute_changes(cik, older["period"], newer["period"],
                              list(holdings_old), list(holdings_new))

    database.insert_changes(conn, changes)
    print_changes(investor_name, older["period"], newer["period"], changes)

    analysis = ""
    if not skip_ai and changes:
        print("Analysoidaan muutokset Claudella...")
        analysis = analyzer.analyze_changes(
            investor_name=investor_name,
            strategy=strategy,
            period_from=older["period"],
            period_to=newer["period"],
            changes=changes,
        )
        save_ai_analysis(conn, cik, older["period"], newer["period"], analysis)
        print("\n" + "─" * 60)
        print("CLAUDE-ANALYYSI")
        print("─" * 60)
        print(analysis)
        print("─" * 60)
        if notify:
            subject = f"13F: {investor_name} {older['period']} → {newer['period']}"
            notifier.send(subject, analysis)

    print(f"\nTietokanta: {database.DB_PATH}")
    return {
        "investor_name": investor_name,
        "period_from": older["period"],
        "period_to": newer["period"],
        "changes": changes,
        "analysis": analysis,
    }


def run_all(skip_ai: bool = False) -> None:
    config_path = Path(__file__).parent.parent / "config" / "investors.yaml"
    if not config_path.exists():
        print("investors.yaml puuttuu.")
        return

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    active = [inv for inv in cfg.get("investors", []) if inv.get("active", False)]
    print(f"Ajetaan {len(active)} aktiivista sijoittajaa...\n")

    results = []
    for inv in active:
        try:
            result = run_tracker(inv["cik"], skip_ai=skip_ai, notify=False)
            if result:
                results.append(result)
        except Exception as e:
            print(f"  Virhe ({inv['name']}): {e}")

    if not results or skip_ai:
        return

    sections = []
    for r in results:
        sep = "=" * 60
        header = f"{sep}\n{r['investor_name'].upper()}  {r['period_from']} → {r['period_to']}\n{sep}"
        sections.append(f"{header}\n\n{r['analysis']}" if r["analysis"] else header)

    body = "\n\n".join(sections)
    today = datetime.date.today().isoformat()
    notifier.send(f"13F Kooste {today}", body)


def main() -> None:
    ap = argparse.ArgumentParser(description="13F Tracker")
    ap.add_argument("--cik", default="0001067983", help="Sijoittajan CIK (oletus: Berkshire)")
    ap.add_argument("--all", action="store_true", dest="all_investors",
                    help="Aja kaikki aktiiviset sijoittajat ja lähetä kooste")
    ap.add_argument("--skip-ai", action="store_true", help="Ohita Claude-analyysi (testaus)")
    args = ap.parse_args()
    if args.all_investors:
        run_all(skip_ai=args.skip_ai)
    else:
        run_tracker(args.cik, skip_ai=args.skip_ai)


if __name__ == "__main__":
    main()
