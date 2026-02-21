"""
==============================================================================
SCRAPER ITBI NITERÓI
Fonte: https://www.fazenda.niteroi.rj.gov.br/site/dados-das-transacoes-imobiliarias/
==============================================================================

FLUXO GERAL:
  1. [ETAPA 1] Descoberta dinâmica dos CSVs na página da Fazenda (BeautifulSoup)
  2. [ETAPA 2] Download dos CSVs anuais (requests)
  3. [ETAPA 3] Consolidação e limpeza dos dados (pandas)
  4. [ETAPA 4] Geocodificação dos logradouros (Nominatim / OpenStreetMap)
  5. [ETAPA 5] Geração do heatmap interativo (Folium) → docs/index.html

INSTALAÇÃO:
  pip install requests beautifulsoup4 pandas folium geopy tqdm

NOTA LEGAL:
  Dados públicos disponibilizados pela Prefeitura de Niterói (SMF).
  Geocodificação via Nominatim exige respeitar rate limit de 1 req/s
  e identificar o user-agent conforme ToS da OSM.

GITHUB PAGES:
  O arquivo gerado em docs/index.html é servido diretamente pelo GitHub Pages.
  Os dados geocodificados ficam em docs/data/ como JSON estático.
==============================================================================
"""

import time
import re
import json
import logging
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
import pandas as pd
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import folium
from folium.plugins import HeatMap
from tqdm import tqdm

# ---------------------------------------------------------------------------
# CONFIGURAÇÕES
# ---------------------------------------------------------------------------

BASE_URL = (
    "https://www.fazenda.niteroi.rj.gov.br/site/dados-das-transacoes-imobiliarias/"
)
DATA_DIR = Path("data/itbi_niteroi")
DOCS_DIR = Path("docs")
OUTPUT_HTML = DOCS_DIR / "index.html"
DATA_JSON = DOCS_DIR / "data" / "itbi_geo.json"
GEOCACHE_CSV = DATA_DIR / "geocache.csv"

# URLs diretas como fallback (caso a descoberta dinâmica falhe)
CSV_URLS_FALLBACK = {
    2020: "https://www.fazenda.niteroi.rj.gov.br/site/wp-content/uploads/2025/02/transacoes_imobiliarias_2020.csv",
    2021: "https://www.fazenda.niteroi.rj.gov.br/site/wp-content/uploads/2025/02/transacoes_imobiliarias_2021.csv",
    2022: "https://www.fazenda.niteroi.rj.gov.br/site/wp-content/uploads/2025/02/transacoes_imobiliarias_2022.csv",
    2023: "https://www.fazenda.niteroi.rj.gov.br/site/wp-content/uploads/2025/02/transacoes_imobiliarias_2023.csv",
    2024: "https://www.fazenda.niteroi.rj.gov.br/site/wp-content/uploads/2025/02/transacoes_imobiliarias_2024.csv",
}

HEADERS = {
    "User-Agent": "ITBIHeatmapNiteroi/1.0 (pesquisa-propria; github.com/seu-usuario/niteroi-itbi-heatmap)",
    "Accept-Language": "pt-BR,pt;q=0.9",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ===========================================================================
# ETAPA 1 — Descoberta dinâmica dos links CSV na página da Fazenda
# ===========================================================================


def descobrir_csv_urls(url: str = BASE_URL) -> dict:
    """
    Acessa a página da SMF Niterói e extrai todos os links .csv presentes.

    Seletor CSS principal: div.entry-content (tema WordPress padrão)
    Padrão do href: *transacoes_imobiliarias_YYYY.csv

    Retorna dict {ano: url_absoluta}.
    Cai no fallback hardcoded se nenhum link for encontrado.
    """
    log.info(f"[ETAPA 1] Acessando: {url}")
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.warning(f"Falha ao acessar página: {e}. Usando fallback.")
        return CSV_URLS_FALLBACK

    soup = BeautifulSoup(resp.text, "html.parser")

    # Seletor CSS: tenta divs de conteúdo comuns em temas WordPress
    content = (
        soup.select_one("div.entry-content")
        or soup.select_one("div.post-content")
        or soup.select_one("main article")
        or soup  # fallback: varre toda a página
    )

    urls = {}
    for tag in content.find_all("a", href=True):
        href = tag["href"]
        match = re.search(r"transacoes_imobiliarias_(\d{4})\.csv", href, re.IGNORECASE)
        if match:
            ano = int(match.group(1))
            urls[ano] = urljoin(url, href)
            log.info(f"  [{ano}] {urls[ano]}")

    if not urls:
        log.warning("Nenhum CSV encontrado dinamicamente — usando URLs hardcoded.")
        return CSV_URLS_FALLBACK

    log.info(f"  {len(urls)} CSVs encontrados.")
    return urls


# ===========================================================================
# ETAPA 2 — Download dos CSVs
# ===========================================================================


def baixar_csvs(csv_urls: dict, destino: Path = DATA_DIR) -> list:
    """
    Faz download de cada CSV anual e salva em destino/.
    Usa cache simples: pula se arquivo já existir.
    """
    log.info("[ETAPA 2] Baixando CSVs...")
    destino.mkdir(parents=True, exist_ok=True)
    arquivos = []

    for ano, url in sorted(csv_urls.items()):
        arquivo = destino / f"transacoes_imobiliarias_{ano}.csv"
        if arquivo.exists():
            log.info(f"  [{ano}] Já existe, pulando.")
            arquivos.append(arquivo)
            continue

        log.info(f"  [{ano}] Baixando: {url}")
        try:
            resp = requests.get(url, headers=HEADERS, timeout=60, stream=True)
            resp.raise_for_status()
            with open(arquivo, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8192):
                    f.write(chunk)
            log.info(f"  [{ano}] Salvo: {arquivo}")
            arquivos.append(arquivo)
            time.sleep(1)
        except requests.RequestException as e:
            log.error(f"  [{ano}] Erro: {e}")

    return arquivos


# ===========================================================================
# ETAPA 3 — Consolidação e limpeza
# ===========================================================================


def carregar_e_consolidar(arquivos: list) -> pd.DataFrame:
    """
    Lê e consolida todos os CSVs em um único DataFrame.

    Encoding: tenta UTF-8 BOM (Windows/Excel) → latin-1.
    Separador: detectado automaticamente (vírgula ou ponto-e-vírgula).
    Limpeza de valores monetários: remove R$, pontos de milhar, troca vírgula→ponto.
    """
    log.info("[ETAPA 3] Consolidando e limpando dados...")
    frames = []

    for arq in arquivos:
        try:
            df = pd.read_csv(arq, encoding="utf-8-sig", sep=None, engine="python")
        except UnicodeDecodeError:
            df = pd.read_csv(arq, encoding="latin-1", sep=None, engine="python")

        df.columns = df.columns.str.strip().str.upper()
        log.info(f"  {arq.name}: {len(df)} linhas, colunas: {list(df.columns)}")
        frames.append(df)

    if not frames:
        raise ValueError("Nenhum CSV carregado.")

    df = pd.concat(frames, ignore_index=True)
    df.dropna(how="all", inplace=True)

    # Limpa colunas numéricas
    for col in df.columns:
        if any(k in col for k in ["VALOR", "ÁREA", "QUANTIDADE"]):
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"[R$\.\s]", "", regex=True)
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Normaliza texto
    for col in ["BAIRRO", "NOME DO LOGRADOURO"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()

    log.info(f"  Total consolidado: {len(df)} linhas")
    return df


# ===========================================================================
# ETAPA 4 — Geocodificação via Nominatim (OpenStreetMap)
# ===========================================================================


def _montar_endereco(row: pd.Series) -> str:
    """Monta string de endereço para geocodificação."""
    logradouro = str(row.get("NOME DO LOGRADOURO", "")).strip()
    bairro = str(row.get("BAIRRO", "")).strip()
    return f"{logradouro}, {bairro}, Niterói, RJ, Brasil"


def geocodificar(df: pd.DataFrame, cache_path: Path = GEOCACHE_CSV) -> pd.DataFrame:
    """
    Geocodifica endereços únicos via Nominatim.

    Estratégia de cache: salva resultados em CSV para evitar re-geocodificação.
    Rate limit: 1.1 req/s (conforme ToS do Nominatim).

    Fallback por endereço não encontrado: tenta só bairro + cidade.
    Para volume grande ou maior precisão, considere HERE/Google Maps API.
    """
    log.info("[ETAPA 4] Geocodificando endereços...")

    geolocator = Nominatim(
        user_agent="ITBIHeatmapNiteroi/1.0 (github.com/seu-usuario/niteroi-itbi-heatmap)",
        timeout=10,
    )
    geocode = RateLimiter(
        geolocator.geocode, min_delay_seconds=1.1, error_wait_seconds=5
    )

    # Carrega cache
    cache = {}
    if cache_path.exists():
        df_cache = pd.read_csv(cache_path)
        cache = {
            row["ENDERECO"]: (row["LAT"], row["LON"]) for _, row in df_cache.iterrows()
        }
        log.info(f"  Cache: {len(cache)} entradas")

    df["ENDERECO"] = df.apply(_montar_endereco, axis=1)
    enderecos_unicos = [e for e in df["ENDERECO"].unique() if e not in cache]
    log.info(f"  {len(enderecos_unicos)} endereços novos para geocodificar")

    novos = {}
    for endereco in tqdm(enderecos_unicos, desc="Geocodificando"):
        try:
            loc = geocode(endereco)
            if loc:
                novos[endereco] = (loc.latitude, loc.longitude)
            else:
                # Fallback: só bairro + cidade
                partes = endereco.split(",")
                fallback = (
                    ", ".join(partes[1:]).strip() if len(partes) > 1 else endereco
                )
                loc2 = geocode(fallback)
                novos[endereco] = (
                    (loc2.latitude, loc2.longitude) if loc2 else (None, None)
                )
        except Exception as e:
            log.warning(f"  Falha '{endereco}': {e}")
            novos[endereco] = (None, None)

    cache.update(novos)

    # Persiste novos no cache
    if novos:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(
            [(k, v[0], v[1]) for k, v in novos.items()],
            columns=["ENDERECO", "LAT", "LON"],
        ).to_csv(
            cache_path,
            mode="a" if cache_path.exists() else "w",
            header=not cache_path.exists(),
            index=False,
        )
        log.info(f"  {len(novos)} novos endereços salvos no cache")

    df["LAT"] = df["ENDERECO"].map(lambda e: cache.get(e, (None, None))[0])
    df["LON"] = df["ENDERECO"].map(lambda e: cache.get(e, (None, None))[1])

    n_ok = df["LAT"].notna().sum()
    log.info(f"  Geocodificados com sucesso: {n_ok}/{len(df)}")

    return df.dropna(subset=["LAT", "LON"])


# ===========================================================================
# ETAPA 5 — Heatmap interativo com Folium
# ===========================================================================


def gerar_heatmap(
    df: pd.DataFrame, output_path: Path = OUTPUT_HTML, json_path: Path = DATA_JSON
) -> None:
    """
    Gera heatmap interativo em HTML com Folium.

    Peso do heatmap: MÉDIA DO VALOR DA TRANSAÇÃO × QUANTIDADE DE TRANSAÇÕES
    → volume financeiro total do logradouro, normalizado [0,1].

    Também exporta JSON com dados geocodificados para uso no GitHub Pages.
    """
    log.info("[ETAPA 5] Gerando heatmap...")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)

    mapa = folium.Map(
        location=[-22.903, -43.113],
        zoom_start=13,
        tiles="CartoDB positron",
    )

    # Detecta coluna de valor
    col_valor = next(
        (c for c in df.columns if "VALOR DA TRANSA" in c),
        next((c for c in df.columns if "VALOR DE AVALIA" in c), None),
    )

    col_qtd = next((c for c in df.columns if "QUANTIDADE" in c), None)

    if col_valor and col_qtd:
        df["PESO"] = df[col_valor].fillna(0) * df[col_qtd].fillna(1)
        max_peso = float(df["PESO"].max())
        df["PESO_NORM"] = (df["PESO"] / max_peso).clip(0, 1) if max_peso > 0 else 0.0
    else:
        df["PESO_NORM"] = 1.0

    heat_data = df[["LAT", "LON", "PESO_NORM"]].astype(float).values.tolist()

    HeatMap(
        heat_data,
        name="Volume financeiro ITBI",
        radius=18,
        blur=15,
        max_zoom=16,
        min_opacity=0.3,
        gradient={0.2: "blue", 0.4: "cyan", 0.6: "lime", 0.8: "yellow", 1.0: "red"},
    ).add_to(mapa)

    # Marcadores clicáveis — itera como dict para tipagem limpa
    for rec in df.to_dict(orient="records"):
        lat = float(rec["LAT"])  # type: ignore[arg-type]
        lon = float(rec["LON"])  # type: ignore[arg-type]
        val_raw = rec.get(col_valor) if col_valor else None
        val_ok = val_raw is not None and val_raw == val_raw  # NaN check sem pandas
        val_str = (
            f"R$ {float(val_raw):,.0f}".replace(",", "X")
            .replace(".", ",")
            .replace("X", ".")
            if val_ok
            else "N/D"
        )
        popup_html = f"""
        <div style="font-family:Arial;font-size:13px;min-width:200px">
          <b>{rec.get("NOME DO LOGRADOURO", "?")}</b><br>
          <i>{rec.get("BAIRRO", "?")}</i><br><br>
          <b>Ano:</b> {rec.get("ANO DO PAGAMENTO DO ITBI", "?")}<br>
          <b>Tipologia:</b> {rec.get("PRINCIPAL TIPOLOGIA", "?")}<br>
          <b>Natureza:</b> {rec.get("PRINCIPAL NATUREZA DA TRANSAÇÃO", "?")}<br>
          <b>Transações:</b> {rec.get(col_qtd, "?") if col_qtd else "?"}<br>
          <b>Valor médio:</b> {val_str}
        </div>"""
        folium.CircleMarker(
            location=[lat, lon],
            radius=4,
            color="#2563eb",
            fill=True,
            fill_opacity=0.5,
            popup=folium.Popup(popup_html, max_width=280),
            tooltip=f"{rec.get('NOME DO LOGRADOURO', '?')} — {val_str}",
        ).add_to(mapa)

    folium.LayerControl().add_to(mapa)
    mapa.save(str(output_path))
    log.info(f"  Heatmap salvo: {output_path}")

    # Exporta JSON para GitHub Pages
    records = df[["LAT", "LON", "BAIRRO", "NOME DO LOGRADOURO", "PESO_NORM"]].to_dict(
        orient="records"
    )
    if col_valor:
        for i, row in enumerate(df.itertuples()):
            records[i]["valor_medio"] = getattr(
                row,
                col_valor.replace(" ", "_")
                .replace("(", "")
                .replace(")", "")
                .replace("$", "")
                .replace("/", ""),
                None,
            )
    json_path.write_text(
        json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info(f"  JSON exportado: {json_path}")


# ===========================================================================
# PIPELINE PRINCIPAL
# ===========================================================================


def main():
    log.info("=" * 60)
    log.info("ITBI Niterói — Pipeline coleta → geocodificação → heatmap")
    log.info("=" * 60)

    csv_urls = descobrir_csv_urls()
    arquivos = baixar_csvs(csv_urls)

    if not arquivos:
        log.error("Nenhum CSV disponível. Abortando.")
        return

    df = carregar_e_consolidar(arquivos)
    df.to_csv(DATA_DIR / "consolidado.csv", index=False, encoding="utf-8-sig")

    df_geo = geocodificar(df)
    df_geo.to_csv(DATA_DIR / "consolidado_geo.csv", index=False, encoding="utf-8-sig")

    gerar_heatmap(df_geo)

    log.info("=" * 60)
    log.info("Pipeline concluído!")
    log.info(f"  Heatmap: {OUTPUT_HTML.resolve()}")
    log.info(f"  JSON:    {DATA_JSON.resolve()}")
    log.info("=" * 60)


if __name__ == "__main__":
    main()
