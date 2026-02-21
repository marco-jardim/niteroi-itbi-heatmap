# AGENTS.md — niteroi-itbi-heatmap

Coding agent reference for this repository. Read before making any change.

---

## Project Overview

Python ETL pipeline that scrapes public ITBI (property transfer tax) data from
Niterói's Municipal Finance Secretariat, geocodes addresses via Nominatim/OSM,
and generates an interactive Folium heatmap published via GitHub Pages.

**Architecture:**
```
scraper.py → consolidado.csv → consolidado_geo.csv → docs/index.html
                                                    → docs/data/itbi_geo.json
```

**Language:** Python 3.10+  
**Entry point:** `python scripts/scraper.py`  
**No JavaScript, no TypeScript, no frontend build step.**

---

## Build / Run Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the full pipeline (scrape → geocode → heatmap)
python scripts/scraper.py

# After pipeline completes, commit the generated output
git add docs/
git commit -m "update: heatmap YYYY-MM-DD"
git push
```

---

## Lint / Format Commands

No linter or formatter is currently configured. Follow these conventions manually:

```bash
# If adding linting, the recommended stack for this project:
pip install ruff black

# Lint (future)
ruff check scripts/

# Format (future)
black scripts/
```

**Until tools are added, adhere to PEP 8 and the patterns in `scripts/scraper.py`.**

---

## Test Commands

No test suite exists yet. There are no `tests/` directories, `test_*.py` files,
or test dependencies in `requirements.txt`.

```bash
# Smoke test: run the pipeline against real URLs
python scripts/scraper.py

# Unit test a single function (when tests are added):
# python -m pytest tests/test_scraper.py::test_descobrir_csv_urls -v

# Run all tests (when added):
# python -m pytest tests/ -v
```

When adding tests: use `pytest`, place files in `tests/`, name them
`test_<module>.py`, and name test functions `test_<function_under_test>`.

---

## Project Structure

```
niteroi-itbi-heatmap/
├── scripts/
│   └── scraper.py          # Single-file ETL pipeline (~450 lines)
├── docs/                   # GitHub Pages root (committed)
│   ├── index.html          # Generated heatmap (Folium output)
│   └── data/
│       └── itbi_geo.json   # Pre-processed geocoded data
├── data/                   # Local cache — NOT committed (.gitignore)
│   └── itbi_niteroi/
│       ├── transacoes_imobiliarias_YYYY.csv
│       ├── consolidado.csv
│       ├── consolidado_geo.csv
│       └── geocache.csv    # Geocoding cache — preserve, never delete
├── PLAN.md                 # Architecture and roadmap
├── README.md
└── requirements.txt
```

**Never commit** the `data/` directory — CSVs are re-downloaded, geocache is
rebuilt automatically. **Always commit** `docs/` after running the pipeline.

---

## Code Style Guidelines

### General

- **PEP 8** throughout. Max line length: 88 characters (Black-compatible).
- Use `snake_case` for all function names, variables, and file names.
- Use `UPPER_SNAKE_CASE` for module-level constants.
- Write docstrings for every public function (Portuguese acceptable).
- Comment section headers with `# ===...===` separator blocks.

### Imports

Follow this import order (one blank line between groups):

```python
# 1. Standard library
import time
import re
import json
import logging
from pathlib import Path
from urllib.parse import urljoin

# 2. Third-party (alphabetical within group)
import requests
from bs4 import BeautifulSoup
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import folium
from folium.plugins import HeatMap
from tqdm import tqdm
```

Never use `from module import *`. Prefer explicit imports.

### Type Hints

Use type hints in all function signatures:

```python
def baixar_csvs(csv_urls: dict, destino: Path = DATA_DIR) -> list:
def geocodificar(df: pd.DataFrame, cache_path: Path = GEOCACHE_CSV) -> pd.DataFrame:
def _montar_endereco(row: pd.Series) -> str:
```

Return types must always be annotated. Parameter types must always be annotated.
Use `Path` (not `str`) for file system paths throughout.

### Naming Conventions

| Symbol kind       | Convention      | Example                        |
|-------------------|-----------------|--------------------------------|
| Functions         | `snake_case`    | `descobrir_csv_urls`           |
| Private helpers   | `_snake_case`   | `_montar_endereco`             |
| Variables         | `snake_case`    | `csv_urls`, `df_cache`         |
| Constants         | `UPPER_SNAKE`   | `BASE_URL`, `DATA_DIR`         |
| DataFrame columns | `UPPER` (match CSV) | `"BAIRRO"`, `"LAT"`, `"LON"` |

Function names may be in Portuguese to match domain language (e.g.,
`geocodificar`, `carregar_e_consolidar`, `gerar_heatmap`).

### Error Handling

Always catch specific exception types — never bare `except:`:

```python
# Good
try:
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
except requests.RequestException as e:
    log.warning(f"Falha ao acessar página: {e}. Usando fallback.")
    return CSV_URLS_FALLBACK

# Bad — never do this
try:
    ...
except:
    pass
```

- Use `log.warning()` for recoverable errors, `log.error()` for fatal ones.
- Always provide a fallback or raise a descriptive `ValueError` when data is missing.
- On geocoding errors, log the address and store `(None, None)` — never crash the pipeline.

### Logging

Use the module-level `log` logger (never `print()` in pipeline code):

```python
log = logging.getLogger(__name__)

log.info("[ETAPA N] Description of stage")
log.warning("Non-fatal issue: details")
log.error("Fatal issue: details")
```

Stage banners use the format `[ETAPA N]` to match existing pipeline structure.

### File I/O

- Always use `pathlib.Path` — never `os.path`.
- Create parent directories with `path.parent.mkdir(parents=True, exist_ok=True)`.
- Use `encoding="utf-8-sig"` when writing CSVs (Excel-compatible BOM).
- When reading CSVs: try `utf-8-sig` first, fall back to `latin-1`.

### HTTP Requests

- Always pass `headers=HEADERS` with a descriptive `User-Agent`.
- Always set `timeout=` (30s for page fetches, 60s for file downloads).
- Always call `resp.raise_for_status()` immediately after a request.
- Respect Nominatim rate limit: 1.1 req/s minimum (`RateLimiter`).

### pandas Conventions

- Normalize column names immediately after loading: `.str.strip().str.upper()`.
- Use `pd.to_numeric(..., errors="coerce")` for monetary columns — never crash on bad data.
- Use `df.dropna(how="all")` before processing, `df.dropna(subset=["LAT", "LON"])` before geocoding output.
- Avoid `.iterrows()` for performance; use `.apply()`, `.map()`, or vectorized ops.
  Exception: `to_dict(orient="records")` + Python loop is acceptable for Folium marker generation.

---

## Key Implementation Notes

- **Geocoding cache** (`geocache.csv`) is the most expensive artifact to rebuild.
  Never delete it. Append new entries; never overwrite.
- **Dynamic CSV discovery** via BeautifulSoup is the primary path; hardcoded
  `CSV_URLS_FALLBACK` dict is the safety net.
- **Heatmap weight** = `VALOR DA TRANSAÇÃO × QUANTIDADE DE TRANSAÇÕES`, normalized
  to `[0, 1]`. This represents total financial volume per street, not price/m².
- `docs/index.html` is a self-contained Folium export (CSS/JS embedded inline).
  Do not edit it manually — always regenerate via `scraper.py`.

---

## External Constraints

- **Nominatim ToS**: max 1 request/second; must identify via `User-Agent`.
- **GitHub Pages**: only `docs/` is published; keep `data/` out of version control.
- **CSV encoding**: source files use Windows-1252 / latin-1; always handle both.
