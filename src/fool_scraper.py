"""Fetch portfolio review posts from Saul's Investing Discussions."""

import re
import time
import requests
from bs4 import BeautifulSoup

BASE_URL = "https://discussion.fool.com"
CATEGORY_JSON = "/c/investment-analysis-clubs/saul-s-investing-discussions/29.json"
_REQUEST_DELAY = 1.0

_s = requests.Session()
_s.headers.update({
    "User-Agent": "13f-tracker/0.1 research-tool",
    "Accept": "application/json",
})

PORTFOLIO_RE = re.compile(r"portfolio.*(review|update)", re.IGNORECASE)


def _get(url: str) -> dict:
    time.sleep(_REQUEST_DELAY)
    r = _s.get(url, timeout=30)
    r.raise_for_status()
    return r.json()


def get_portfolio_posts(usernames: set[str]) -> list[dict]:
    """Return recent portfolio review posts by tracked usernames."""
    data = _get(f"{BASE_URL}{CATEGORY_JSON}")

    users = {u["id"]: u["username"] for u in data.get("users", [])}

    results = []
    for topic in data.get("topic_list", {}).get("topics", []):
        if not PORTFOLIO_RE.search(topic.get("title", "")):
            continue

        op_username = None
        for poster in topic.get("posters", []):
            if "Original Poster" in (poster.get("description") or ""):
                op_username = users.get(poster.get("user_id"))
                break
        if op_username is None and topic.get("posters"):
            op_username = users.get(topic["posters"][0].get("user_id"))

        if op_username not in usernames:
            continue

        topic_id = topic["id"]
        slug = topic.get("slug", str(topic_id))
        results.append({
            "topic_id": topic_id,
            "slug": slug,
            "title": topic["title"],
            "url": f"{BASE_URL}/t/{slug}/{topic_id}",
            "username": op_username,
            "created_at": topic.get("created_at", ""),
        })

    return results


def get_post_text(topic_id: int, slug: str) -> str:
    """Return plain text of the first post in a topic."""
    data = _get(f"{BASE_URL}/t/{slug}/{topic_id}.json")
    posts = data.get("post_stream", {}).get("posts", [])
    if not posts:
        return ""
    cooked = posts[0].get("cooked", "")
    return BeautifulSoup(cooked, "html.parser").get_text(separator="\n", strip=True)
