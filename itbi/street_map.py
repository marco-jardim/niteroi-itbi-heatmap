"""
street_map.py — Mapa de ruas coloridas por score, preço médio e quantidade de negociações.

Gera docs/street_map.html com 3 layers Folium PolyLine (toggleáveis) e um painel
de ranking das ruas mais caras/baratas. Sem HeatMap.
"""

from __future__ import annotations

import json
import logging
import re
import unicodedata
from pathlib import Path
from typing import Optional

import folium
import pandas as pd
import branca.element

log = logging.getLogger(__name__)

# ── Constantes ────────────────────────────────────────────────────────────────

OSM_CACHE_PATH = Path("data/itbi_niteroi/osm_niteroi.graphml")

# Escala divergente Azul→Branco→Vermelho (12 passos)
# Azul escuro = valor baixo | Vermelho escuro = valor alto
# Perceptualmente uniforme e intuitivo para dados de intensidade urbana
_CORES = [
    "#053061",  # azul muito escuro
    "#2166ac",  # azul escuro
    "#4393c3",  # azul médio
    "#92c5de",  # azul claro
    "#d1e5f0",  # azul muito claro
    "#f7f7f7",  # branco (valor mediano)
    "#fddbc7",  # vermelho muito claro
    "#f4a582",  # vermelho claro
    "#d6604d",  # vermelho médio
    "#b2182b",  # vermelho escuro
    "#67001f",  # vermelho muito escuro
]

COL_VALOR = "MÉDIA DO VALOR DA TRANSAÇÃO (R$)"
COL_QTDE = "QUANTIDADE DE TRANSAÇÕES"
COL_LOG = "NOME DO LOGRADOURO"

# Expansão de abreviações de tipo de logradouro (ITBI → OSM)
_ABREV = {
    r"\br\b\.?": "rua",
    r"\bav\b\.?": "avenida",
    r"\best\b\.?": "estrada",
    r"\brod\b\.?": "rodovia",
    r"\btrv\b\.?": "travessa",
    r"\bal\b\.?": "alameda",
    r"\bpc\b\.?": "praca",
    r"\bpraca\b": "praca",
    r"\blgo\b\.?": "largo",
    r"\bvla\b\.?": "vila",
    r"\bte\b\.?": "travessa",
    r"\bcj\b\.?": "conjunto",
}

# Sufixos honoríficos a remover (ITBI usa vírgula + título)
_HONORIFICOS = re.compile(
    r",\s*(dr|dra|prof|profa|dep|sen|cel|brig|gal|pe|frei|dom|"
    r"cons|jorn|acd|mq|pres|eng|arq|des|min|maj|cap|ten|sgt"
    r"|cte|al|bel|rev|mons|gen|marechal)\b.*$",
    re.IGNORECASE,
)

# Artigos invertidos: "rua criancas,das" → "rua das criancas"
_ARTIGO_INVERTIDO = re.compile(
    r"^(.*?),\s*(das?|dos?|de|do|d[ao]s?|e)\s*$", re.IGNORECASE
)


# ── Helpers ──────────────────────────────────────────────────────────────────


def _norm(texto: object) -> str:
    """Normaliza texto para matching: expande abreviações, remove honoríficos e acentos."""
    if not isinstance(texto, str):
        texto = "" if texto is None else str(texto)

    # 1. Remove acentos e converte para minúsculas
    nfkd = unicodedata.normalize("NFD", texto)
    s = "".join(c for c in nfkd if unicodedata.category(c) != "Mn")
    s = s.lower().strip()

    # 2. Remove sufixos honoríficos (ex: ",dr", ",prof")
    s = _HONORIFICOS.sub("", s).strip().rstrip(",").strip()

    # 3. Inverte artigos invertidos: "rua criancas,das" → "rua das criancas"
    m = _ARTIGO_INVERTIDO.match(s)
    if m:
        s = f"{m.group(2)} {m.group(1)}".strip()

    # 4. Expande abreviações de tipo de logradouro
    for padrao, expansao in _ABREV.items():
        s = re.sub(padrao, expansao, s, count=1)

    # 5. Normaliza espaços
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _cor(valor_norm: float) -> str:
    """Retorna cor hex para valor normalizado [0,1]."""
    idx = min(int(valor_norm * 11), 10)
    return _CORES[idx]


def _extrair_nome_osm(name_field: object) -> str:
    """Extrai string do campo 'name' do OSM (pode ser str ou list)."""
    if isinstance(name_field, list):
        return name_field[0] if name_field else ""
    return name_field if isinstance(name_field, str) else ""


def _carregar_grafo(cache: Path = OSM_CACHE_PATH):  # type: ignore[return]
    """Carrega grafo OSM do cache local."""
    try:
        import osmnx as ox
    except ImportError as e:
        raise ImportError("osmnx não instalado. Execute: pip install osmnx") from e

    if not cache.exists():
        raise FileNotFoundError(
            f"Grafo OSM não encontrado: {cache}\n"
            'Execute: python -c "import osmnx as ox; '
            "ox.save_graphml(ox.graph_from_place('Niteroi, Rio de Janeiro, Brasil', "
            "network_type='drive'), 'data/itbi_niteroi/osm_niteroi.graphml')\""
        )
    log.info("  Carregando grafo OSM: %s", cache)
    import osmnx as ox  # noqa: F811

    return ox.load_graphml(cache)


def _segmentos_por_nome(G) -> dict[str, list[list[tuple[float, float]]]]:  # type: ignore[type-arg]
    """
    Retorna dict {nome_norm: [[coords_aresta1], [coords_aresta2], ...]}
    onde cada lista de coords é [(lat, lon), ...].
    """
    try:
        import osmnx as ox
    except ImportError as e:
        raise ImportError("osmnx não instalado.") from e

    _, edges = ox.graph_to_gdfs(G, nodes=True, edges=True)
    result: dict[str, list[list[tuple[float, float]]]] = {}

    for _, row in edges.iterrows():
        nome_raw = _extrair_nome_osm(row.get("name", ""))
        if not nome_raw:
            continue
        nome_n = _norm(nome_raw)
        geom = row.get("geometry")
        if geom is None:
            continue
        coords = [(lat, lon) for lon, lat in geom.coords]
        result.setdefault(nome_n, []).append(coords)

    return result


def _casar(
    itbi_logradouros: list[str], segmentos: dict[str, list[list[tuple[float, float]]]]
) -> dict[str, list[list[tuple[float, float]]]]:
    """
    Casa nomes ITBI → segmentos OSM.
    Estratégia: exact match > substring.
    """
    matched: dict[str, list[list[tuple[float, float]]]] = {}
    osm_keys = list(segmentos.keys())

    for log_itbi in itbi_logradouros:
        n = _norm(log_itbi)
        if not n:
            continue
        # Exact
        if n in segmentos:
            matched[log_itbi] = segmentos[n]
            continue
        # Substring
        candidatos = [k for k in osm_keys if n in k or k in n]
        if candidatos:
            # Pega o mais longo (mais específico)
            melhor = max(candidatos, key=len)
            matched[log_itbi] = segmentos[melhor]

    return matched


def _normalizar_serie(s: pd.Series) -> pd.Series:
    """Normaliza série para [0,1]. Retorna 0.5 se constante."""
    mn, mx = s.min(), s.max()
    if mx == mn:
        return pd.Series(0.5, index=s.index)
    return (s - mn) / (mx - mn)


def _layer_polilinhas(
    nome_layer: str,
    logradouros_df: pd.DataFrame,
    col_valor: str,
    matches: dict[str, list[list[tuple[float, float]]]],
    peso: float = 4.0,
    show: bool = True,
) -> folium.FeatureGroup:
    """Cria um FeatureGroup com PolyLines coloridas por col_valor."""
    df = logradouros_df[[COL_LOG, col_valor]].dropna()
    norm = _normalizar_serie(df[col_valor])
    df = df.copy()
    df["_norm"] = norm.values

    fg = folium.FeatureGroup(name=nome_layer, show=show)
    matched_count = 0

    for _, row in df.iterrows():
        log_nome = row[COL_LOG]
        if log_nome not in matches:
            continue
        cor = _cor(float(row["_norm"]))
        val_fmt = (
            f"R$ {row[col_valor]:,.0f}".replace(",", "X")
            .replace(".", ",")
            .replace("X", ".")
            if col_valor == COL_VALOR
            else f"{int(row[col_valor]):,}".replace(",", ".")
        )
        tooltip = f"<b>{log_nome}</b><br>{nome_layer}: {val_fmt}"
        for segmento in matches[log_nome]:
            folium.PolyLine(
                locations=segmento,
                color=cor,
                weight=peso,
                opacity=0.85,
                tooltip=tooltip,
            ).add_to(fg)
        matched_count += 1

    log.info(
        "  Layer '%s': %d/%d ruas com geometria OSM", nome_layer, matched_count, len(df)
    )
    return fg


def _painel_ranking(df_preco: pd.DataFrame, n: int = 10) -> str:
    """Gera HTML do painel lateral com top/bottom ruas por preço médio."""
    df = df_preco[[COL_LOG, COL_VALOR, COL_QTDE]].dropna()
    df = df.sort_values(COL_VALOR, ascending=False)
    top = df.head(n)
    bot = df.tail(n).iloc[::-1]

    def fmt_valor(v: float) -> str:
        return f"R$ {v:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")

    def linhas(rows: pd.DataFrame, cor: str) -> str:
        out = ""
        for i, (_, r) in enumerate(rows.iterrows(), 1):
            out += (
                f'<tr><td style="color:{cor};font-weight:bold">{i}º</td>'
                f'<td style="max-width:160px;overflow:hidden;white-space:nowrap;'
                f'text-overflow:ellipsis" title="{r[COL_LOG]}">{r[COL_LOG]}</td>'
                f'<td style="text-align:right">{fmt_valor(r[COL_VALOR])}</td>'
                f'<td style="text-align:right">{int(r[COL_QTDE])}</td></tr>'
            )
        return out

    tabela_css = (
        "font-size:11px;border-collapse:collapse;width:100%;font-family:sans-serif"
    )
    th_css = "padding:3px 5px;border-bottom:1px solid #ccc;text-align:left"

    html = f"""
<div id="ranking-panel" style="
    position:fixed;top:80px;right:10px;z-index:1000;
    background:rgba(255,255,255,0.95);border-radius:8px;
    box-shadow:0 2px 8px rgba(0,0,0,0.3);padding:12px;
    max-height:80vh;overflow-y:auto;min-width:340px;
">
  <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
    <b style="font-family:sans-serif;font-size:13px">Ranking de Ruas — Preço Médio</b>
    <button onclick="document.getElementById('ranking-panel').style.display='none'"
      style="border:none;background:transparent;cursor:pointer;font-size:16px">&times;</button>
  </div>
  <p style="font-family:sans-serif;font-size:11px;color:#666;margin:0 0 8px">
    Valor médio das transações ITBI 2020–2024
  </p>
  <b style="font-family:sans-serif;font-size:11px;color:#1a9850">▲ Top {n} mais caras</b>
  <table style="{tabela_css}">
    <tr>
      <th style="{th_css}">#</th>
      <th style="{th_css}">Rua</th>
      <th style="{th_css};text-align:right">Preço Médio</th>
      <th style="{th_css};text-align:right">Neg.</th>
    </tr>
    {linhas(top, "#1a9850")}
  </table>
  <br>
  <b style="font-family:sans-serif;font-size:11px;color:#d73027">▼ Top {n} mais baratas</b>
  <table style="{tabela_css}">
    <tr>
      <th style="{th_css}">#</th>
      <th style="{th_css}">Rua</th>
      <th style="{th_css};text-align:right">Preço Médio</th>
      <th style="{th_css};text-align:right">Neg.</th>
    </tr>
    {linhas(bot, "#d73027")}
  </table>
</div>
<button onclick="document.getElementById('ranking-panel').style.display='block'"
  id="btn-ranking"
  style="position:fixed;top:80px;right:10px;z-index:999;display:none;
         padding:6px 12px;background:#333;color:#fff;border:none;
         border-radius:6px;cursor:pointer;font-family:sans-serif">
  ☰ Ranking
</button>
<script>
  document.getElementById('ranking-panel').addEventListener('transitionend', function() {{
    if (this.style.display==='none') document.getElementById('btn-ranking').style.display='block';
  }});
  document.querySelector('#ranking-panel button').addEventListener('click', function() {{
    document.getElementById('btn-ranking').style.display='block';
  }});
</script>
"""
    return html


def _legenda_html(titulo: str) -> str:
    faixas = [
        ("0–10%", _CORES[0]),
        ("10–30%", _CORES[2]),
        ("30–50%", _CORES[4]),
        ("50–70%", _CORES[6]),
        ("70–90%", _CORES[8]),
        ("90–100%", _CORES[9]),
    ]
    itens = "".join(
        f'<div style="display:flex;align-items:center;gap:6px;margin:2px 0">'
        f'<div style="width:24px;height:8px;background:{cor};border-radius:2px"></div>'
        f'<span style="font-size:11px;font-family:sans-serif">{label}</span></div>'
        for label, cor in faixas
    )
    return (
        f'<div style="position:fixed;bottom:30px;left:10px;z-index:1000;'
        f"background:rgba(255,255,255,0.92);padding:10px 14px;border-radius:8px;"
        f'box-shadow:0 2px 6px rgba(0,0,0,0.25)">'
        f'<b style="font-family:sans-serif;font-size:12px">{titulo}</b><br><br>'
        f"{itens}</div>"
    )


# ── Função principal ──────────────────────────────────────────────────────────


def gerar_street_map(
    df_geo: pd.DataFrame,
    insights_path: Optional[Path] = None,
    output_path: Path = Path("docs/street_map.html"),
    score_col: str = "score_valorizacao",
    janela: int = 36,
    osm_cache: Path = OSM_CACHE_PATH,
    ranking_n: int = 15,
) -> Path:
    """
    Gera mapa de ruas coloridas com 3 layers (score, preço, quantidade).

    Parâmetros
    ----------
    df_geo : DataFrame com colunas NOME DO LOGRADOURO, MÉDIA DO VALOR DA TRANSAÇÃO,
             QUANTIDADE DE TRANSAÇÕES, LAT, LON.
    insights_path : JSON de insights (itbi_insights.json).
    output_path : caminho do HTML de saída.
    score_col : 'score_valorizacao' ou 'score_joia_escondida'.
    janela : janela temporal em meses (12, 24 ou 36).
    osm_cache : graphml pré-baixado.
    ranking_n : número de ruas no painel de ranking.
    """
    # ── 1. Agrega métricas por logradouro ────────────────────────────────────
    log.info("  Agregando métricas por logradouro...")
    df_geo.columns = [c.strip() for c in df_geo.columns]

    col_valor_real = next(
        (c for c in df_geo.columns if "VALOR DA TRANSA" in c.upper()), COL_VALOR
    )
    col_qtde_real = next(
        (c for c in df_geo.columns if "QUANTIDADE" in c.upper()), COL_QTDE
    )
    col_log_real = next(
        (c for c in df_geo.columns if "LOGRADOURO" in c.upper()), COL_LOG
    )

    df_geo[col_valor_real] = pd.to_numeric(df_geo[col_valor_real], errors="coerce")
    df_geo[col_qtde_real] = pd.to_numeric(df_geo[col_qtde_real], errors="coerce")

    agg = (
        df_geo.groupby(col_log_real)
        .agg(
            preco_medio=(col_valor_real, "mean"),
            qtde_total=(col_qtde_real, "sum"),
            lat=("LAT", "mean"),
            lon=("LON", "mean"),
        )
        .reset_index()
        .rename(columns={col_log_real: COL_LOG})
    )
    agg[COL_VALOR] = agg["preco_medio"]
    agg[COL_QTDE] = agg["qtde_total"]

    log.info("  %d ruas únicas no dataset", len(agg))

    # ── 2. Score de valorização dos insights ─────────────────────────────────
    score_df: Optional[pd.DataFrame] = None
    if insights_path and Path(insights_path).exists():
        log.info("  Carregando insights: %s", insights_path)
        raw = json.loads(Path(insights_path).read_text(encoding="utf-8"))
        # suporta {"insights": [...]} e {"logradouro": [...]}
        if isinstance(raw, dict):
            todos = raw.get("insights", raw.get("logradouro", []))
        else:
            todos = raw
        ins = pd.DataFrame.from_records(todos)
        # filtra nivel=logradouro e janela correta
        if "nivel" in ins.columns:
            ins = ins[ins["nivel"] == "logradouro"]
        if "janela_meses" in ins.columns:
            ins = ins[ins["janela_meses"] == janela]
        # campo do nome da rua pode ser "regiao" ou "logradouro"
        nome_col = "regiao" if "regiao" in ins.columns else "logradouro"
        if score_col in ins.columns and nome_col in ins.columns:
            ins[score_col] = pd.to_numeric(ins[score_col], errors="coerce")
            ins = ins[[nome_col, score_col]].dropna()
            ins = ins.rename(columns={nome_col: COL_LOG})
            score_df = ins
            log.info("  %d ruas com %s (janela=%dm)", len(score_df), score_col, janela)
    else:
        log.warning("  insights_path não encontrado; layer de score omitido.")

    # Merge score com agg
    if score_df is not None:
        agg = agg.merge(score_df, on=COL_LOG, how="left")
    else:
        agg[score_col] = float("nan")

    # ── 3. Carrega grafo OSM e extrai segmentos ───────────────────────────────
    log.info("  Carregando grafo OSM...")
    G = _carregar_grafo(osm_cache)
    log.info("  Extraindo segmentos por nome de rua...")
    segmentos = _segmentos_por_nome(G)
    log.info("  %d nomes únicos no grafo OSM", len(segmentos))

    # ── 4. Casa logradouros ITBI ↔ segmentos OSM ─────────────────────────────
    log.info("  Casando logradouros ITBI com geometrias OSM...")
    matches = _casar(agg[COL_LOG].dropna().tolist(), segmentos)
    log.info("  %d/%d logradouros com geometria OSM", len(matches), len(agg))

    # ── 5. Mapa Folium ────────────────────────────────────────────────────────
    lat_center = float(df_geo["LAT"].dropna().mean())
    lon_center = float(df_geo["LON"].dropna().mean())
    m = folium.Map(
        location=[lat_center, lon_center], zoom_start=13, tiles="CartoDB positron"
    )

    # Layer 1 — Preço médio (visível por padrão)
    fg_preco = _layer_polilinhas(
        "Preço Médio (R$)", agg, COL_VALOR, matches, peso=5.0, show=True
    )
    fg_preco.add_to(m)

    # Layer 2 — Quantidade de negociações
    fg_qtde = _layer_polilinhas(
        "Qtde. Negociações", agg, COL_QTDE, matches, peso=5.0, show=False
    )
    fg_qtde.add_to(m)

    # Layer 3 — Score de valorização (apenas se disponível)
    if score_df is not None and not agg[score_col].isna().all():
        fg_score = _layer_polilinhas(
            f"Score Valorização ({janela}m)",
            agg,
            score_col,
            matches,
            peso=5.0,
            show=False,
        )
        fg_score.add_to(m)

    folium.LayerControl(collapsed=False).add_to(m)

    # ── 6. Painel de ranking ──────────────────────────────────────────────────
    painel_html = _painel_ranking(agg, n=ranking_n)
    m.get_root().html.add_child(branca.element.Element(painel_html))

    # ── 7. Legenda ────────────────────────────────────────────────────────────
    legenda_html = _legenda_html("Escala (vermelho=baixo, verde=alto)")
    m.get_root().html.add_child(branca.element.Element(legenda_html))

    # ── 8. Salva ──────────────────────────────────────────────────────────────
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    m.save(str(output_path))
    log.info("  Street map salvo: %s", output_path)
    return output_path
