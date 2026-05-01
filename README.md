# 13F Tracker

Seuraa institutionaalisten sijoittajien salkkumuutoksia SEC EDGARista ja analysoi ne Claude API:lla.

## Asennus

```bash
cd 13f-tracker
python -m venv .venv
.venv\Scripts\activate        # Windows
# tai: source .venv/bin/activate  (Mac/Linux)
pip install -e .
```

## Konfiguraatio

```bash
cp .env.example .env
# Muokkaa .env: lisää EDGAR_USER_AGENT ja ANTHROPIC_API_KEY
```

## Käyttö

### Vaihe 1 — Hae ja vertaa Berkshire Hathawayn raportit

```bash
python -m src.tracker --cik 0001067983
```

Muut sijoittajat (CIK `config/investors.yaml`):
```bash
python -m src.tracker --cik 0001336528   # Bill Ackman
python -m src.tracker --cik 0001569205   # Terry Smith
```

## Rakenne

```
config/
  investors.yaml       # Seurattavat sijoittajat
src/
  edgar_client.py      # SEC EDGAR API
  parser.py            # 13F XML -parseri
  database.py          # SQLite
  tracker.py           # Pääohjelma
data/
  filings.db           # SQLite-tietokanta (gitignored)
```

## Tietokanta

SQLite: `data/filings.db`
- `investors` — sijoittajat
- `filings` — 13F-raportit
- `holdings` — omistukset per raportti
- `changes` — lasketut muutokset kvartaalien välillä
