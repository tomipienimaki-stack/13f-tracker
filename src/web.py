"""Flask web application for 13F Tracker."""

from pathlib import Path
from flask import Flask, render_template, abort
from src import database
from src import fool_parser as fparser

app = Flask(__name__, template_folder=str(Path(__file__).parent.parent / "templates"))


def _get_conn():
    conn = database.connect()
    database.init_schema(conn)
    database.init_fool_schema(conn)
    return conn


@app.route("/")
def index():
    conn = _get_conn()

    investors_raw = conn.execute(
        "SELECT * FROM investors ORDER BY name"
    ).fetchall()

    investors = []
    for inv in investors_raw:
        filings = database.get_two_latest_filings(conn, inv["cik"])
        counts = {"new_count": 0, "increase_count": 0, "decrease_count": 0, "exit_count": 0}
        latest_period = None
        if len(filings) == 2:
            latest_period = filings[0]["period"]
            rows = conn.execute(
                "SELECT change_type, COUNT(*) as cnt FROM changes WHERE cik=? AND period_to=? GROUP BY change_type",
                (inv["cik"], filings[0]["period"]),
            ).fetchall()
            for r in rows:
                key = {"NEW": "new_count", "INCREASE": "increase_count",
                       "DECREASE": "decrease_count", "EXIT": "exit_count"}.get(r["change_type"])
                if key:
                    counts[key] = r["cnt"]
        investors.append({**dict(inv), "latest_period": latest_period, **counts})

    sauls_raw = conn.execute(
        "SELECT DISTINCT username FROM fool_portfolios ORDER BY username"
    ).fetchall()
    sauls_posters = []
    for row in sauls_raw:
        username = row["username"]
        latest = conn.execute(
            "SELECT * FROM fool_portfolios WHERE username=? ORDER BY period DESC LIMIT 1",
            (username,),
        ).fetchone()
        sauls_posters.append({
            "username": username,
            "latest_period": latest["period"] if latest else None,
            "ytd_return": latest["ytd_return"] if latest else None,
            "narrative": latest["narrative"] if latest else None,
        })

    return render_template("index.html", investors=investors, sauls_posters=sauls_posters)


@app.route("/investor/<cik>")
def investor(cik: str):
    conn = _get_conn()

    inv = conn.execute("SELECT * FROM investors WHERE cik=?", (cik,)).fetchone()
    if not inv:
        abort(404)

    filings = database.get_two_latest_filings(conn, cik)
    holdings_counts = {}
    for f in filings:
        holdings_counts[f["id"]] = conn.execute(
            "SELECT COUNT(*) FROM holdings WHERE filing_id=?", (f["id"],)
        ).fetchone()[0]

    top_holdings = []
    changes = []
    analysis = None

    if filings:
        top_holdings = conn.execute(
            "SELECT * FROM holdings WHERE filing_id=? ORDER BY value_usd DESC LIMIT 20",
            (filings[0]["id"],),
        ).fetchall()

    if len(filings) == 2:
        changes = conn.execute(
            """SELECT * FROM changes WHERE cik=? AND period_from=? AND period_to=?
               ORDER BY ABS(value_delta) DESC""",
            (cik, filings[1]["period"], filings[0]["period"]),
        ).fetchall()
        analysis_row = conn.execute(
            "SELECT ai_analysis FROM changes WHERE cik=? AND period_to=? AND ai_analysis IS NOT NULL LIMIT 1",
            (cik, filings[0]["period"]),
        ).fetchone()
        if analysis_row:
            analysis = analysis_row["ai_analysis"]

    return render_template(
        "investor.html",
        investor=inv,
        filings=filings,
        holdings_counts=holdings_counts,
        top_holdings=top_holdings,
        changes=changes,
        analysis=analysis,
    )


@app.route("/sauls")
def sauls():
    conn = _get_conn()

    usernames = [r["username"] for r in conn.execute(
        "SELECT DISTINCT username FROM fool_portfolios ORDER BY username"
    ).fetchall()]

    posters = []
    poster_holdings = {}
    for username in usernames:
        latest = conn.execute(
            "SELECT * FROM fool_portfolios WHERE username=? ORDER BY period DESC LIMIT 1",
            (username,),
        ).fetchone()
        if not latest:
            continue
        holdings = database.get_fool_holdings(conn, latest["id"])
        poster_holdings[username] = list(holdings)
        posters.append({
            "username": username,
            "latest_period": latest["period"],
            "ytd_return": latest["ytd_return"],
            "top3": list(holdings)[:3],
        })

    # Build comparison table
    all_tickers: dict[str, str] = {}
    for holdings in poster_holdings.values():
        for h in holdings:
            if h["ticker"] not in all_tickers:
                all_tickers[h["ticker"]] = h["name"] or ""

    def avg_pct(ticker):
        vals = [h["pct"] for hs in poster_holdings.values() for h in hs if h["ticker"] == ticker]
        return sum(vals) / len(vals) if vals else 0

    comparison = []
    for ticker in sorted(all_tickers, key=avg_pct, reverse=True):
        row = {"ticker": ticker, "name": all_tickers[ticker]}
        for username, holdings in poster_holdings.items():
            pct_map = {h["ticker"]: h["pct"] for h in holdings}
            row[username] = pct_map.get(ticker)
        comparison.append(row)

    return render_template("sauls.html", posters=posters, comparison=comparison)


@app.route("/sauls/<username>")
def sauls_poster(username: str):
    conn = _get_conn()

    all_portfolios = conn.execute(
        "SELECT * FROM fool_portfolios WHERE username=? ORDER BY period DESC",
        (username,),
    ).fetchall()
    if not all_portfolios:
        abort(404)

    latest = all_portfolios[0]
    holdings = database.get_fool_holdings(conn, latest["id"])

    changes = []
    prev_period = None
    if len(all_portfolios) >= 2:
        prev = all_portfolios[1]
        prev_period = prev["period"]
        h_new = [dict(r) for r in database.get_fool_holdings(conn, latest["id"])]
        h_old = [dict(r) for r in database.get_fool_holdings(conn, prev["id"])]
        changes = fparser.compute_changes(username, prev["period"], latest["period"], h_old, h_new)

    return render_template(
        "sauls_poster.html",
        username=username,
        latest=latest,
        all_portfolios=all_portfolios,
        holdings=holdings,
        changes=changes,
        prev_period=prev_period,
    )


if __name__ == "__main__":
    app.run(debug=True, port=5000)
