"""Track Saul's Investing Discussions portfolio posts.

Käyttö:
    python -m src.fool_tracker
    python -m src.fool_tracker --skip-ai
"""

import argparse
import datetime
import io
import sys
from pathlib import Path

if sys.stdout.encoding != "utf-8":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

import yaml

from src import database, notifier
from src import fool_scraper as scraper
from src import fool_parser as fparser


def _fmt_change(c: dict) -> str:
    label = f"{c['ticker']} ({c['name']})" if c.get("name") else c["ticker"]
    label = label.ljust(30)
    if c["change_type"] == "NEW":
        return f"  {label}  NEW     {c['pct_new']:.1f}%"
    elif c["change_type"] == "EXIT":
        return f"  {label}  EXIT"
    else:
        sign = "+" if c["pct_delta"] > 0 else ""
        return f"  {label}  {sign}{c['pct_delta']:.1f}pp  → {c['pct_new']:.1f}%"


def run_fool_tracker(skip_ai: bool = False) -> None:
    config_path = Path(__file__).parent.parent / "config" / "fool_posters.yaml"
    if not config_path.exists():
        print("fool_posters.yaml puuttuu.")
        return

    with open(config_path) as f:
        cfg = yaml.safe_load(f)

    active = [p for p in cfg.get("posters", []) if p.get("active", False)]
    usernames = {p["username"] for p in active}

    conn = database.connect()
    database.init_fool_schema(conn)

    print(f"Haetaan Saul's-päivitykset ({len(active)} postaajaa)...")
    posts = scraper.get_portfolio_posts(usernames)
    print(f"  Löydettiin {len(posts)} portfolio review -ketjua.\n")

    # Step 1: fetch and parse any new posts
    for post in posts:
        username = post["username"]
        url = post["url"]
        title = post["title"]
        topic_id = post["topic_id"]
        slug = post["slug"]

        existing = conn.execute(
            "SELECT id FROM fool_portfolios WHERE username=? AND thread_url=?",
            (username, url),
        ).fetchone()
        if existing:
            print(f"  [{username}] {title} — jo käsitelty, ohitetaan.")
            continue

        print(f"  [{username}] {title}")
        raw_text = scraper.get_post_text(topic_id, slug)
        if not raw_text:
            print("    Tyhjä teksti, ohitetaan.")
            continue

        if skip_ai:
            print("    --skip-ai, ohitetaan parsinta.")
            continue

        print("    Parsitaan Claudella...")
        try:
            data = fparser.parse_portfolio(raw_text, username, title)
        except Exception as e:
            print(f"    Parsintavirhe: {e}")
            continue

        period = data.get("period") or ""
        if not period:
            print("    Periodi puuttuu, ohitetaan.")
            continue

        portfolio_id = database.upsert_fool_portfolio(
            conn, username, period, url, raw_text,
            data.get("ytd_return"), data.get("narrative"),
        )
        database.insert_fool_holdings(conn, portfolio_id, data.get("holdings") or [])
        print(f"    Tallennettu: {username} {period}")

    if skip_ai:
        return

    # Step 2: build digest from latest stored portfolio per poster
    print()
    all_sections = []
    for username in sorted(usernames):
        db_portfolios = database.get_fool_two_latest(conn, username)
        if not db_portfolios:
            continue

        latest = db_portfolios[0]
        sep = "=" * 55
        lines = [sep, f"  {username.upper()}  {latest['period']}", sep]

        if latest["ytd_return"] is not None:
            lines.append(f"  YTD: {latest['ytd_return']:+.1f}%")

        # Top holdings
        holdings = database.get_fool_holdings(conn, latest["id"])
        if holdings:
            lines.append("\nTOP 10 OMISTUKSET:")
            for h in list(holdings)[:10]:
                label = f"{h['ticker']} ({h['name']})" if h["name"] else h["ticker"]
                lines.append(f"  {label.ljust(30)}  {h['pct']:.1f}%")

        # Month-over-month changes
        if len(db_portfolios) >= 2:
            older = db_portfolios[1]
            h_new = [dict(r) for r in database.get_fool_holdings(conn, latest["id"])]
            h_old = [dict(r) for r in database.get_fool_holdings(conn, older["id"])]
            changes = fparser.compute_changes(
                username, older["period"], latest["period"], h_old, h_new,
            )
            if changes:
                groups = {"NEW": [], "INCREASE": [], "DECREASE": [], "EXIT": []}
                for c in changes:
                    groups[c["change_type"]].append(c)
                labels = {
                    "NEW": "UUDET",
                    "INCREASE": "LISÄYKSET (>2pp)",
                    "DECREASE": "VÄHENNYKSET (>2pp)",
                    "EXIT": "EXITIT",
                }
                for ct, label in labels.items():
                    rows = sorted(groups[ct], key=lambda x: abs(x["pct_delta"]), reverse=True)
                    if rows:
                        lines.append(f"\n{label}:")
                        lines.extend(_fmt_change(r) for r in rows)

        if latest["narrative"]:
            lines.append(f"\n{latest['narrative']}")

        section = "\n".join(lines)
        print(section + "\n")
        all_sections.append(section)

    if not all_sections:
        print("Ei dataa lähetettäväksi.")
        return

    # Summary table: all portfolios side by side, largest to smallest position
    poster_holdings = {}
    for username in sorted(usernames):
        db_portfolios = database.get_fool_two_latest(conn, username)
        if db_portfolios:
            poster_holdings[username] = list(database.get_fool_holdings(conn, db_portfolios[0]["id"]))

    if poster_holdings:
        col_w = 28
        posters_sorted = sorted(poster_holdings.keys())
        header = "TICKER + YHTIÖ".ljust(col_w) + "  " + "  ".join(u.upper().ljust(8) for u in posters_sorted)
        divider = "-" * len(header)

        # Collect all tickers across all portfolios
        all_tickers: dict[str, str] = {}
        for holdings in poster_holdings.values():
            for h in holdings:
                if h["ticker"] not in all_tickers:
                    all_tickers[h["ticker"]] = h["name"] or ""

        # Sort by average position size descending
        def avg_pct(ticker: str) -> float:
            vals = [h["pct"] for holdings in poster_holdings.values() for h in holdings if h["ticker"] == ticker]
            return sum(vals) / len(vals) if vals else 0

        tickers_sorted = sorted(all_tickers, key=avg_pct, reverse=True)

        table_lines = ["\n" + "=" * len(header), "OMISTUSVERTAILU", "=" * len(header), header, divider]
        for ticker in tickers_sorted:
            name = all_tickers[ticker]
            label = f"{ticker} ({name})" if name else ticker
            row = label.ljust(col_w) + "  "
            cols = []
            for username in posters_sorted:
                pct_map = {h["ticker"]: h["pct"] for h in poster_holdings[username]}
                cols.append(f"{pct_map[ticker]:.1f}%".ljust(8) if ticker in pct_map else "-".ljust(8))
            row += "  ".join(cols)
            table_lines.append(row)

        table = "\n".join(table_lines)
        print(table + "\n")
        all_sections.append(table)

    body = "\n\n".join(all_sections)
    today = datetime.date.today().isoformat()
    notifier.send(f"Saul's Board Kooste {today}", body)
    print("[Email] Kooste lähetetty.")


def main() -> None:
    ap = argparse.ArgumentParser(description="Saul's Board Tracker")
    ap.add_argument("--skip-ai", action="store_true", help="Ohita Claude-parsinta")
    args = ap.parse_args()
    run_fool_tracker(skip_ai=args.skip_ai)


if __name__ == "__main__":
    main()
