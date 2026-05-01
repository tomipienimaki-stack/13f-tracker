"""Parse Saul's board portfolio posts with Claude."""

import json
import os
import re
from pathlib import Path

import anthropic
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env", override=True)

_client = None


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        _client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
    return _client


def parse_portfolio(raw_text: str, username: str, title: str) -> dict:
    """Use Claude to extract structured portfolio data from post text."""
    prompt = f"""Extract structured data from this Saul's Investing Discussions portfolio post.

Title: {title}
Username: {username}

Post text:
{raw_text[:4000]}

Return ONLY valid JSON with this exact structure:
{{
  "period": "YYYY-MM",
  "ytd_return": -9.4,
  "monthly_returns": {{"Jan": -5.5, "Feb": -17.5}},
  "holdings": [{{"ticker": "SHOP", "name": "Shopify", "pct": 9.5}}, ...],
  "recent_activity": "brief description of trades",
  "watchlist": ["TICKER1", "TICKER2"],
  "narrative": "1-2 sentence summary of key themes and outlook"
}}

Rules:
- period: derive from title (e.g. "April 2026" -> "2026-04")
- ytd_return: the final YTD return as a float (e.g. -9.4)
- holdings: all positions with ticker, full company name, and percentage, sorted by pct descending
- If a field is not present in the post, use null or empty array
- Return ONLY the JSON object, no markdown fences or other text"""

    response = _get_client().messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        messages=[{"role": "user", "content": prompt}],
    )

    text = response.content[0].text.strip()
    text = re.sub(r"^```json\s*|\s*```$", "", text, flags=re.MULTILINE).strip()
    return json.loads(text)


def compute_changes(
    username: str,
    period_from: str,
    period_to: str,
    holdings_old: list[dict],
    holdings_new: list[dict],
    threshold_pp: float = 2.0,
) -> list[dict]:
    """Compare two months' holdings and return notable changes."""
    old = {h["ticker"]: h for h in holdings_old}
    new = {h["ticker"]: h for h in holdings_new}
    changes = []

    for ticker, h in new.items():
        pct = h["pct"]
        name = h.get("name") or ""
        if ticker not in old:
            changes.append({
                "username": username, "period_from": period_from, "period_to": period_to,
                "ticker": ticker, "name": name, "change_type": "NEW",
                "pct_new": pct, "pct_delta": pct,
            })
        else:
            delta = pct - old[ticker]["pct"]
            if abs(delta) >= threshold_pp:
                changes.append({
                    "username": username, "period_from": period_from, "period_to": period_to,
                    "ticker": ticker, "name": name,
                    "change_type": "INCREASE" if delta > 0 else "DECREASE",
                    "pct_new": pct, "pct_delta": delta,
                })

    for ticker, h in old.items():
        if ticker not in new:
            changes.append({
                "username": username, "period_from": period_from, "period_to": period_to,
                "ticker": ticker, "name": h.get("name") or "",
                "change_type": "EXIT", "pct_new": 0.0, "pct_delta": -h["pct"],
            })

    return changes
