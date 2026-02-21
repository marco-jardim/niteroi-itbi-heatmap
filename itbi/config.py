"""
Constantes e caminhos centralizados para o pacote ITBI Niterói.

Todas as demais etapas do pipeline devem importar daqui — nunca definir
constantes localmente para evitar divergências.
"""

from pathlib import Path

# ===========================================================================
# Caminhos
# ===========================================================================

#: Diretório de cache local (não commitado — .gitignore)
DATA_DIR: Path = Path("data/itbi_niteroi")

#: Diretório publicado pelo GitHub Pages (commitado)
DOCS_DIR: Path = Path("docs")

#: Heatmap gerado pelo Folium
OUTPUT_HTML: Path = DOCS_DIR / "index.html"

#: JSON pré-processado para GitHub Pages
DATA_JSON: Path = DOCS_DIR / "data" / "itbi_geo.json"

#: Cache de geocodificação — NUNCA deletar; só append
GEOCACHE_CSV: Path = DATA_DIR / "geocache.csv"

# ===========================================================================
# URLs e HTTP
# ===========================================================================

#: Página da SMF Niterói com os links de download
BASE_URL: str = (
    "https://www.fazenda.niteroi.rj.gov.br/site/dados-das-transacoes-imobiliarias/"
)

#: Fallback hardcoded usado quando a descoberta dinâmica falha
CSV_URLS_FALLBACK: dict[int, str] = {
    2020: "https://www.fazenda.niteroi.rj.gov.br/site/wp-content/uploads/2025/02/transacoes_imobiliarias_2020.csv",
    2021: "https://www.fazenda.niteroi.rj.gov.br/site/wp-content/uploads/2025/02/transacoes_imobiliarias_2021.csv",
    2022: "https://www.fazenda.niteroi.rj.gov.br/site/wp-content/uploads/2025/02/transacoes_imobiliarias_2022.csv",
    2023: "https://www.fazenda.niteroi.rj.gov.br/site/wp-content/uploads/2025/02/transacoes_imobiliarias_2023.csv",
    2024: "https://www.fazenda.niteroi.rj.gov.br/site/wp-content/uploads/2025/02/transacoes_imobiliarias_2024.csv",
}

#: Headers HTTP — identificação obrigatória pelo ToS do Nominatim e boa prática geral
HEADERS: dict[str, str] = {
    "User-Agent": (
        "ITBIHeatmapNiteroi/1.0 "
        "(pesquisa-propria; github.com/seu-usuario/niteroi-itbi-heatmap)"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9",
}

# ===========================================================================
# Geocodificação (Nominatim / OpenStreetMap)
# ===========================================================================

#: User-Agent identificador para o Nominatim (ToS exige string descritiva)
NOMINATIM_USER_AGENT: str = (
    "ITBIHeatmapNiteroi/1.0 (github.com/seu-usuario/niteroi-itbi-heatmap)"
)

#: Delay mínimo entre chamadas ao Nominatim (1 req/s conforme ToS)
NOMINATIM_DELAY: float = 1.1
