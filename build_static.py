"""Generate static HTML site from local SQLite database.

Käyttö:
    python build_static.py

Generoi docs/-hakemiston, jonka voi pushata GitHubiin ja julkaista
GitHub Pagesin kautta.
"""

import shutil
from pathlib import Path
from src.web import app
from src import database

OUT_DIR = Path(__file__).parent / "docs"
BASE_HREF = "/13f-tracker/"


def write(rel_path: str, content: bytes) -> None:
    dest = OUT_DIR / rel_path.lstrip("/")
    dest.parent.mkdir(parents=True, exist_ok=True)
    html = content.decode("utf-8").replace(
        "</head>", f'<base href="{BASE_HREF}"></head>', 1
    )
    dest.write_bytes(html.encode("utf-8"))


def main() -> None:
    if OUT_DIR.exists():
        shutil.rmtree(OUT_DIR)
    OUT_DIR.mkdir()

    # Prevent GitHub Pages from processing with Jekyll
    (OUT_DIR / ".nojekyll").touch()

    conn = database.connect()

    with app.test_client() as c:
        write("index.html", c.get("/").data)
        print("  /")

        write("sauls/index.html", c.get("/sauls").data)
        print("  /sauls")

        for inv in conn.execute("SELECT cik FROM investors").fetchall():
            cik = inv["cik"]
            write(f"investor/{cik}/index.html", c.get(f"/investor/{cik}").data)
            print(f"  /investor/{cik}")

        for row in conn.execute("SELECT DISTINCT username FROM fool_portfolios").fetchall():
            u = row["username"]
            write(f"sauls/{u}/index.html", c.get(f"/sauls/{u}").data)
            print(f"  /sauls/{u}")

    print(f"\nValmis: {OUT_DIR}")


if __name__ == "__main__":
    main()
