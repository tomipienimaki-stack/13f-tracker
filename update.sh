#!/usr/bin/env bash
set -e

PYTHON=".venv/Scripts/python.exe"

echo "=== Haetaan 13F-raportit ==="
$PYTHON -m src.tracker --all

echo ""
echo "=== Haetaan Saul's Board ==="
$PYTHON -m src.fool_tracker

echo ""
echo "=== Generoidaan staattinen sivusto ==="
$PYTHON build_static.py

echo ""
echo "=== Pushataan GitHubiin ==="
git add docs/
git diff --cached --quiet && echo "Ei muutoksia docs/:ssa, ohitetaan commit." || \
  git commit -m "Update: $(date '+%Y-%m-%d')" && git push origin main

echo ""
echo "Valmis."
