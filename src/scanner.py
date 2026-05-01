"""
13F Scanner — ajaa kaikki aktiiviset sijoittajat ja etsii konvergenssit.

Käyttö:
    python -m src.scanner
    python -m src.scanner --skip-ai
    python -m src.scanner --skip-notify
"""

import argparse
import io
import sys
from pathlib import Path

import yaml

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

from src import analyzer, database
from src.tracker import run_tracker


def _latest_period_pair(conn) -> tuple[str, str] | tuple[None, None]:
    row = conn.execute("""
        SELECT period_from, period_to, COUNT(DISTINCT cik) AS n
        FROM changes
        GROUP BY period_from, period_to
        ORDER BY period_to DESC, n DESC
        LIMIT 1
    """).fetchone()
    return (row["period_from"], row["period_to"]) if row else (None, None)


def find_convergences(conn, period_from: str, period_to: str) -> list[dict]:
    """Osakkeet joissa 2+ sijoittajaa teki saman NEW/EXIT-liikkeen."""
    rows = conn.execute("""
        SELECT c.cusip,
               c.company_name,
               c.change_type,
               GROUP_CONCAT(DISTINCT i.name) AS investors,
               COUNT(DISTINCT c.cik)      AS investor_count,
               SUM(c.value_delta)         AS total_value_delta
        FROM changes c
        JOIN investors i ON c.cik = i.cik
        WHERE c.period_from = ? AND c.period_to = ?
          AND c.change_type IN ('NEW', 'EXIT')
        GROUP BY c.cusip, c.change_type
        HAVING investor_count >= 2
        ORDER BY investor_count DESC, ABS(total_value_delta) DESC
    """, (period_from, period_to)).fetchall()
    return [dict(r) for r in rows]


def _fmt_val(v: int) -> str:
    v = abs(v)
    if v >= 1_000_000_000:
        return f"${v/1e9:.1f}B"
    if v >= 1_000_000:
        return f"${v/1e6:.0f}M"
    return f"${v/1e3:.0f}K"


def _analyze_convergences(convergences: list[dict], period_from: str, period_to: str) -> str:
    lines = [
        f"  - {c['company_name']} ({c['change_type']}, {c['investor_count']}× "
        f"{_fmt_val(c['total_value_delta'] or 0)}): {c['investors']}"
        for c in convergences
    ]
    prompt = f"""Analysoi 13F-konvergenssit ({period_from} → {period_to}):

Nämä osakkeet esiintyvät useammalla institutionaalisella sijoittajalla:
{chr(10).join(lines)}

Tehtävä:
1. Mikä yhteinen teema tai makroskenaario yhdistää näitä konvergensseja? (max 150 sanaa)
2. Mitkä konvergenssit ovat merkittävimpiä ja miksi? (max 150 sanaa)
3. Mitä tämä kertoo instituutioiden kollektiivisesta näkemyksestä? (max 100 sanaa)

Vastaa suomeksi. Vältä spekulaatiota — perustele datalla."""

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


def scan_all(skip_ai: bool = False, skip_notify: bool = False) -> None:
    config_path = Path(__file__).parent.parent / "config" / "investors.yaml"
    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    active = [inv for inv in cfg.get("investors", []) if inv.get("active", False)]
    print(f"\nSkannataan {len(active)} aktiivista sijoittajaa...\n")

    failed = []
    for inv in active:
        print(f"\n{'─' * 60}")
        try:
            run_tracker(inv["cik"], skip_ai=skip_ai)
        except Exception as exc:
            print(f"  VIRHE ({inv['name']}): {exc}")
            failed.append(inv["name"])

    if failed:
        print(f"\nEpäonnistuneet: {', '.join(failed)}")

    # Konvergenssianalyysi
    conn = database.connect()
    period_from, period_to = _latest_period_pair(conn)
    if not period_from:
        print("\nEi dataa konvergenssianalyysiin.")
        return

    convergences = find_convergences(conn, period_from, period_to)

    print(f"\n{'='*60}")
    print(f"  KONVERGENSSI  {period_from} → {period_to}")
    print(f"{'='*60}")

    if not convergences:
        print("\n  Ei yhteisiä NEW/EXIT-liikkeitä useammalla sijoittajalla.")
        print()
        return

    print(f"\n  Sama liike {period_from} → {period_to}:\n")
    for c in convergences:
        name = (c["company_name"] or "?")[:35].ljust(35)
        ct = c["change_type"].ljust(6)
        n = c["investor_count"]
        print(f"  {name}  {ct}  {n}×  —  {c['investors']}")

    if not skip_ai:
        print("\nAnalysoidaan konvergenssi Claudella...")
        analysis = _analyze_convergences(convergences, period_from, period_to)
        print("\n" + "─" * 60)
        print("KONVERGENSSIANALYYSI")
        print("─" * 60)
        print(analysis)
        print("─" * 60)

        if not skip_notify:
            from src import notifier
            notifier.send(
                subject=f"13F Konvergenssi {period_from} → {period_to}",
                body=analysis,
            )

    print()


def main() -> None:
    ap = argparse.ArgumentParser(description="13F Scanner — kaikki sijoittajat + konvergenssi")
    ap.add_argument("--skip-ai", action="store_true", help="Ohita Claude-analyysi")
    ap.add_argument("--skip-notify", action="store_true", help="Ohita ilmoitukset")
    args = ap.parse_args()
    scan_all(skip_ai=args.skip_ai, skip_notify=args.skip_notify)


if __name__ == "__main__":
    main()
