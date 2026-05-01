import os
from pathlib import Path
import anthropic
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env", override=True)

_client = None

SYSTEM_PROMPT = """Olet kokenut sijoitusanalyytikko, joka erikoistuu institutionaalisten sijoittajien \
salkkuanalyysin. Analysoit 13F-ilmoitusten muutoksia tarkasti ja perustat väitteesi dataan. \
Vältät spekulaatiota — jos syy ei ole selvä datasta, sanot sen suoraan. Vastaat aina suomeksi."""


def _get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:
        api_key = os.getenv("ANTHROPIC_API_KEY")
        if not api_key:
            raise RuntimeError("ANTHROPIC_API_KEY not set in .env")
        _client = anthropic.Anthropic(api_key=api_key)
    return _client


def _fmt_changes(changes: list[dict]) -> dict[str, list[str]]:
    groups: dict[str, list[str]] = {"NEW": [], "INCREASE": [], "DECREASE": [], "EXIT": []}
    for c in changes:
        name = c.get("company_name") or c.get("cusip") or "?"
        val = abs(c.get("value_delta", 0))
        if val >= 1_000_000_000:
            val_str = f"${val/1e9:.1f}B"
        elif val >= 1_000_000:
            val_str = f"${val/1e6:.0f}M"
        else:
            val_str = f"${val/1e3:.0f}K"
        groups[c["change_type"]].append(f"  - {name}: {val_str}")
    return groups


def analyze_changes(
    investor_name: str,
    strategy: str,
    period_from: str,
    period_to: str,
    changes: list[dict],
) -> str:
    """Call Claude API to analyze portfolio changes. Returns Finnish analysis text."""
    if not changes:
        return "Ei merkittäviä muutoksia analysoitavana."

    groups = _fmt_changes(changes)

    def section(label: str, items: list[str]) -> str:
        if not items:
            return ""
        return f"{label}:\n" + "\n".join(items) + "\n"

    changes_text = (
        section("UUDET POSITIOT", groups["NEW"])
        + section("LISÄYKSET (>10%)", groups["INCREASE"])
        + section("VÄHENNYKSET (>10%)", groups["DECREASE"])
        + section("EXITIT", groups["EXIT"])
    ).strip()

    user_prompt = f"""Analysoi {investor_name}n ({strategy}) 13F-muutokset ({period_from} → {period_to}):

{changes_text}

Tehtävä:
1. Mikä on todennäköisin teesin muutos näiden takana? (max 200 sanaa)
2. Mitkä 2-3 liikettä ovat merkittävimpiä? Miksi? (max 200 sanaa)
3. Mitkä vaativat lisätutkimusta? (max 100 sanaa)

Vältä spekulaatiota. Perustele datalla. Vastaa suomeksi."""

    client = _get_client()

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=2048,
        system=[
            {
                "type": "text",
                "text": SYSTEM_PROMPT,
                "cache_control": {"type": "ephemeral"},
            }
        ],
        messages=[{"role": "user", "content": user_prompt}],
    )

    usage = response.usage
    cached = getattr(usage, "cache_read_input_tokens", 0)
    created = getattr(usage, "cache_creation_input_tokens", 0)
    total_in = usage.input_tokens
    total_out = usage.output_tokens
    print(
        f"  [Claude] in={total_in} out={total_out} "
        f"cache_read={cached} cache_create={created}"
    )

    return response.content[0].text
