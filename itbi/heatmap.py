"""
Etapa 5 — Geração do heatmap interativo com Folium.

Fase 4 adiciona:

* Filtro client-side por **ano** (``ANO DO PAGAMENTO DO ITBI``) e **bairro**
  via painel de controles injetado no HTML gerado.
* **Painel de estatísticas** flutuante (total de transações, bairro mais
  ativo, valor médio global) que se atualiza ao aplicar filtros.
* Suporte a camada **choropleth** sobreposta ao heatmap, ativada quando um
  GeoJSON local é fornecido via ``geojson_bairros``; alternável pelo
  LayerControl nativo do Folium.
* Campos ``ANO DO PAGAMENTO DO ITBI`` e a coluna de quantidade incluídos no
  JSON exportado para o GitHub Pages.

Uso standalone::

    python -m itbi.heatmap
    python -m itbi.heatmap --no-markers
    python -m itbi.heatmap --choropleth-geojson bairros.geojson
    python -m itbi.heatmap --choropleth-key nome_bairro
    python -m itbi.heatmap --output outro.html
"""

import json
import logging
from pathlib import Path

import folium
from branca.element import Element
from folium.plugins import HeatMap
import pandas as pd

from itbi.config import DATA_DIR, DATA_JSON, OUTPUT_HTML

log = logging.getLogger(__name__)

# Colunas base incluídas no JSON exportado para o GitHub Pages
_COLUNAS_JSON_BASE: list[str] = [
    "LAT",
    "LON",
    "BAIRRO",
    "NOME DO LOGRADOURO",
    "PESO_NORM",
    "NIVEL_GEO",
]

# ===========================================================================
# Template HTML/CSS/JS do painel de filtros e estatísticas
#
# Placeholder __PONTOS__ é substituído pelo array JSON dos pontos em
# _construir_controles_filtro(). Não usar f-string neste bloco para evitar
# escape de chaves de CSS e JS.
# ===========================================================================
_CONTROLES_TEMPLATE: str = """\
<!-- ===== ITBI Filtros + Estatísticas ===== -->
<style>
#itbi-panel {
  position: absolute; top: 10px; right: 10px; z-index: 1001;
  background: rgba(255,255,255,.97); border-radius: 8px;
  padding: 14px 16px; box-shadow: 0 2px 10px rgba(0,0,0,.25);
  font-family: Arial, sans-serif; font-size: 13px;
  min-width: 212px; max-width: 244px;
}
#itbi-panel h3 {
  margin: 0 0 10px; font-size: 14px; font-weight: 700;
  color: #1e3a5f; border-bottom: 2px solid #2563eb; padding-bottom: 6px;
}
#itbi-panel label {
  display: block; margin: 8px 0 2px; font-size: 11px; font-weight: 700;
  color: #444; text-transform: uppercase; letter-spacing: .04em;
}
#itbi-panel select {
  width: 100%; padding: 5px 6px; border: 1px solid #ccc;
  border-radius: 4px; background: #fafafa; font-size: 12px; cursor: pointer;
}
#itbi-panel select:hover { border-color: #2563eb; }
#itbi-stats {
  margin-top: 12px; padding-top: 10px; border-top: 1px solid #e5e7eb;
}
.isr {
  display: flex; justify-content: space-between;
  align-items: baseline; margin: 5px 0;
}
.isl { color: #6b7280; font-size: 11px; }
.isv {
  font-weight: 700; color: #1e3a5f; font-size: 12px; text-align: right;
  max-width: 130px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}
@media (max-width: 600px) {
  #itbi-panel { top: auto; bottom: 30px; right: 5px; left: 5px; max-width: none; }
}
</style>
<div id="itbi-panel">
  <h3>&#x1F5FA;&#xFE0F;&nbsp;Filtros ITBI</h3>
  <label for="itbi-ano">Ano</label>
  <select id="itbi-ano"><option value="">Todos os anos</option></select>
  <label for="itbi-bairro">Bairro</label>
  <select id="itbi-bairro"><option value="">Todos os bairros</option></select>
  <div id="itbi-stats">
    <div class="isr">
      <span class="isl">Transações:</span>
      <span class="isv" id="is-total">—</span>
    </div>
    <div class="isr">
      <span class="isl">Bairro + ativo:</span>
      <span class="isv" id="is-bairro">—</span>
    </div>
    <div class="isr">
      <span class="isl">Valor médio:</span>
      <span class="isv" id="is-valor">—</span>
    </div>
  </div>
</div>
<script>
(function () {
  'use strict';
  var PTS = __PONTOS__;
  var yr = '', br = '';

  /* ── Localiza o mapa Leaflet criado pelo Folium ── */
  function findMap() {
    var m = null;
    document.querySelectorAll('[id^="map_"]').forEach(function (el) {
      var c = window[el.id];
      if (!m && c && typeof c.eachLayer === 'function') m = c;
    });
    return m;
  }

  /* ── Filtra pontos de acordo com os selects ativos ── */
  function filt() {
    return PTS.filter(function (p) {
      return (!yr || String(p.ano) === yr) && (!br || p.bairro === br);
    });
  }

  /* ── Atualiza o Leaflet.heat existente via setLatLngs() ──
     Manter a camada original preserva o toggle no LayerControl. */
  function updateHeat(lm, pts) {
    var hLayer = null;
    lm.eachLayer(function (l) {
      /* Identifica HeatMap pelo canvas com classe CSS do leaflet-heat */
      if (!hLayer && l._canvas &&
          l._canvas.classList &&
          l._canvas.classList.contains('leaflet-heatmap-layer')) {
        hLayer = l;
      }
    });
    var hData = pts.map(function (p) { return [p.lat, p.lon, p.peso_norm || 0.5]; });
    if (hLayer) {
      hLayer.setLatLngs(hData);
    } else if (hData.length && typeof L !== 'undefined' && L.heatLayer) {
      /* Fallback: recria a camada se a original não for encontrada */
      L.heatLayer(hData, {
        radius: 18, blur: 15, maxZoom: 16, minOpacity: 0.3,
        gradient: { 0.2: 'blue', 0.4: 'cyan', 0.6: 'lime', 0.8: 'yellow', 1.0: 'red' }
      }).addTo(lm);
    }
  }

  /* ── Atualiza o painel de estatísticas ── */
  function updateStats(pts) {
    var tot = pts.reduce(function (s, p) { return s + (p.qtd || 1); }, 0);
    var cnt = {};
    pts.forEach(function (p) {
      var b = p.bairro || 'N/D';
      cnt[b] = (cnt[b] || 0) + (p.qtd || 1);
    });
    var top = Object.keys(cnt).sort(function (a, b) {
      return cnt[b] - cnt[a];
    })[0] || 'N/D';
    var vs = pts.filter(function (p) { return p.valor_medio > 0; });
    var med = vs.length
      ? vs.reduce(function (s, p) { return s + p.valor_medio; }, 0) / vs.length
      : null;
    var g = function (id) { return document.getElementById(id); };
    if (g('is-total'))  g('is-total').textContent  = tot.toLocaleString('pt-BR');
    if (g('is-bairro')) g('is-bairro').textContent = top;
    if (g('is-valor'))  g('is-valor').textContent  =
      med ? 'R\u00a0' + Math.round(med).toLocaleString('pt-BR') : 'N/D';
  }

  /* ── Aplica filtros: atualiza heatmap + stats ── */
  function run() {
    var lm = findMap();
    if (!lm) return;
    var pts = filt();
    updateHeat(lm, pts);
    updateStats(pts);
  }

  /* ── Preenche os selects e registra event listeners ── */
  function populate() {
    var anos = {}, bairros = {};
    PTS.forEach(function (p) {
      if (p.ano != null)    anos[p.ano]       = 1;
      if (p.bairro != null) bairros[p.bairro] = 1;
    });
    var asel = document.getElementById('itbi-ano');
    if (asel) {
      Object.keys(anos).map(Number).sort().forEach(function (a) {
        var o = document.createElement('option');
        o.value = a; o.textContent = a; asel.appendChild(o);
      });
      asel.addEventListener('change', function () { yr = this.value; run(); });
    }
    var bsel = document.getElementById('itbi-bairro');
    if (bsel) {
      Object.keys(bairros).sort().forEach(function (b) {
        var o = document.createElement('option');
        o.value = b; o.textContent = b; bsel.appendChild(o);
      });
      bsel.addEventListener('change', function () { br = this.value; run(); });
    }
  }

  /* ── Ponto de entrada com retry enquanto o mapa Leaflet carrega ── */
  function init() {
    if (!findMap()) { setTimeout(init, 200); return; }
    populate();
    run();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { setTimeout(init, 150); });
  } else {
    setTimeout(init, 150);
  }
})();
</script>
<!-- ===== /ITBI Filtros + Estatísticas ===== -->
"""


# ===========================================================================
# Auxiliares privados
# ===========================================================================


def _detect_col(df: pd.DataFrame, *fragments: str) -> str | None:
    """Retorna o primeiro nome de coluna que contém todos os fragmentos.

    Args:
        df:        DataFrame cujas colunas serão inspecionadas.
        *fragments: Substrings que devem aparecer no nome da coluna.

    Returns:
        Nome da coluna encontrada ou ``None`` se nenhuma bater.
    """
    for col in df.columns:
        if all(f in col for f in fragments):
            return col
    return None


def _safe_val(val: object) -> object:
    """Converte valores NaN/numpy para tipos nativos Python (compatível com JSON).

    Args:
        val: Qualquer valor de célula de DataFrame.

    Returns:
        ``None`` se NaN/NA; escalar Python nativo caso contrário.
    """
    if val is None:
        return None
    try:
        if pd.isna(val):  # type: ignore[arg-type]
            return None
    except (TypeError, ValueError):
        pass
    # numpy scalar → Python nativo (ex.: np.int64 → int, np.float64 → float)
    item_fn = getattr(val, "item", None)
    if callable(item_fn):
        return item_fn()
    return val


def _agregar_por_bairro(
    df: pd.DataFrame,
    col_valor: str | None,
    col_qtd: str | None,
) -> pd.DataFrame:
    """Agrega dados por bairro para uso na camada choropleth.

    Args:
        df:        DataFrame geocodificado com coluna ``BAIRRO``.
        col_valor: Nome da coluna de valor da transação (ou ``None``).
        col_qtd:   Nome da coluna de quantidade de transações (ou ``None``).

    Returns:
        DataFrame com ``BAIRRO``, ``TOTAL_TRANSACOES`` e
        opcionalmente ``VALOR_MEDIO``.
    """
    agg: dict[str, tuple] = {}
    if col_qtd:
        agg["TOTAL_TRANSACOES"] = (col_qtd, "sum")
    else:
        agg["TOTAL_TRANSACOES"] = ("PESO_NORM", "count")
    if col_valor:
        agg["VALOR_MEDIO"] = (col_valor, "mean")
    result: pd.DataFrame = (  # type: ignore[assignment]
        df.dropna(subset=["BAIRRO"]).groupby("BAIRRO", as_index=False).agg(**agg)
    )
    return result


def _construir_pontos_js(
    df: pd.DataFrame,
    col_valor: str | None,
    col_qtd: str | None,
) -> str:
    """Serializa os pontos geocodificados como array JSON para o filtro JS.

    Retorna string JSON minificada com chaves em minúsculo:
    ``lat``, ``lon``, ``bairro``, ``ano``, ``qtd``, ``valor_medio``,
    ``peso_norm``.

    Args:
        df:        DataFrame com pelo menos ``LAT``, ``LON``, ``BAIRRO``,
                   ``PESO_NORM``.
        col_valor: Nome da coluna de valor da transação (pode ser ``None``).
        col_qtd:   Nome da coluna de quantidade de transações (pode ser
                   ``None``).

    Returns:
        String JSON minificada, sem NaN, compatível com ``JSON.parse``.
    """
    col_ano = _detect_col(df, "ANO", "PAGAMENTO")

    col_map: dict[str, str] = {
        "lat": "LAT",
        "lon": "LON",
        "bairro": "BAIRRO",
        "peso_norm": "PESO_NORM",
    }
    if col_ano and col_ano in df.columns:
        col_map["ano"] = col_ano
    if col_qtd and col_qtd in df.columns:
        col_map["qtd"] = col_qtd
    if col_valor and col_valor in df.columns:
        col_map["valor_medio"] = col_valor

    available_src = {k: v for k, v in col_map.items() if v in df.columns}
    js_df = df[[v for v in available_src.values()]].copy()
    js_df.columns = list(available_src.keys())

    for c in ["lat", "lon", "peso_norm", "valor_medio"]:
        if c in js_df.columns:
            js_df[c] = pd.to_numeric(js_df[c], errors="coerce")
    for c in ["ano", "qtd"]:
        if c in js_df.columns:
            js_df[c] = pd.to_numeric(js_df[c], errors="coerce")

    records: list[dict] = [
        {k: _safe_val(v) for k, v in row.items()}
        for row in js_df.to_dict(orient="records")  # type: ignore[call-overload]
    ]
    return json.dumps(records, ensure_ascii=False, separators=(",", ":"))


def _construir_controles_filtro(pontos_js: str) -> str:
    """Retorna bloco HTML (estilo + div + script) do painel de filtros.

    Substitui o placeholder ``__PONTOS__`` pelo array JSON serializado.

    Args:
        pontos_js: Saída de :func:`_construir_pontos_js`.

    Returns:
        String HTML pronta para ser injetada via
        :class:`branca.element.Element`.
    """
    return _CONTROLES_TEMPLATE.replace("__PONTOS__", pontos_js)


# ===========================================================================
# Etapa 5 — Heatmap
# ===========================================================================


def gerar_heatmap(
    df: pd.DataFrame,
    output_path: Path = OUTPUT_HTML,
    json_path: Path = DATA_JSON,
    incluir_marcadores: bool = True,
    geojson_bairros: Path | None = None,
    choropleth_key: str = "nome",
) -> None:
    """Gera heatmap interativo em HTML com Folium e exporta JSON de dados.

    O peso de cada ponto é ``VALOR DA TRANSAÇÃO × QUANTIDADE DE TRANSAÇÕES``
    (volume financeiro total do logradouro), normalizado para ``[0, 1]``.

    **Fase 4 — novas funcionalidades:**

    * Painel flutuante com filtros de **ano** e **bairro** client-side;
      atualiza o heatmap via ``L.HeatLayer.setLatLngs()`` sem reload.
    * **Painel de estatísticas** (total de transações, bairro mais ativo,
      valor médio) sincronizado com os filtros.
    * Camada **choropleth** opcional por bairro (requer GeoJSON local com
      propriedade ``choropleth_key``); alternável pelo LayerControl.
    * JSON exportado inclui ``ANO DO PAGAMENTO DO ITBI`` e coluna de
      quantidade de transações.

    Args:
        df:                  DataFrame geocodificado (obrigatório: ``LAT``,
                             ``LON``).
        output_path:         Arquivo HTML de saída (cria diretórios pai).
        json_path:           Arquivo JSON de saída para GitHub Pages.
        incluir_marcadores:  Se ``False``, omite os
                             :class:`~folium.CircleMarker` clicáveis —
                             útil para volumes > 5 000 registros.
        geojson_bairros:     Caminho para GeoJSON local dos bairros. Se
                             ``None`` ou não existir, choropleth é omitido
                             sem erro.
        choropleth_key:      Propriedade GeoJSON usada para correlacionar
                             nomes de bairros (padrão: ``"nome"``).
    """
    log.info("[ETAPA 5] Gerando heatmap...")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.parent.mkdir(parents=True, exist_ok=True)

    mapa = folium.Map(
        location=[-22.903, -43.113],
        zoom_start=13,
        tiles="CartoDB positron",
    )

    # -----------------------------------------------------------------------
    # Detecta colunas de valor e quantidade
    # -----------------------------------------------------------------------
    col_valor: str | None = _detect_col(df, "VALOR DA TRANSA") or _detect_col(
        df, "VALOR DE AVALIA"
    )
    col_qtd: str | None = _detect_col(df, "QUANTIDADE")

    # -----------------------------------------------------------------------
    # Calcula peso normalizado
    # -----------------------------------------------------------------------
    df = df.copy()
    if col_valor and col_qtd:
        df["PESO"] = df[col_valor].fillna(0) * df[col_qtd].fillna(1)
        max_peso = float(df["PESO"].max())  # type: ignore[arg-type]
        df["PESO_NORM"] = (df["PESO"] / max_peso).clip(0, 1) if max_peso > 0 else 0.0
    else:
        log.warning(
            "  Colunas de valor/quantidade não encontradas; peso = 1.0 uniforme."
        )
        df["PESO_NORM"] = 1.0

    # -----------------------------------------------------------------------
    # Camada HeatMap (estática inicial — JS atualiza via setLatLngs após load)
    # -----------------------------------------------------------------------
    heat_data: list[list[float]] = (
        df[["LAT", "LON", "PESO_NORM"]]
        .apply(pd.to_numeric, errors="coerce")
        .dropna()
        .values.tolist()
    )
    HeatMap(
        heat_data,
        name="Volume financeiro ITBI",
        radius=18,
        blur=15,
        max_zoom=16,
        min_opacity=0.3,
        gradient={0.2: "blue", 0.4: "cyan", 0.6: "lime", 0.8: "yellow", 1.0: "red"},
    ).add_to(mapa)

    # -----------------------------------------------------------------------
    # Choropleth por bairro (opcional — Fase 4)
    # -----------------------------------------------------------------------
    if geojson_bairros is not None:
        if geojson_bairros.exists():
            df_bairro = _agregar_por_bairro(df, col_valor, col_qtd)
            geojson_str = geojson_bairros.read_text(encoding="utf-8")
            folium.Choropleth(
                geo_data=geojson_str,
                name="Choropleth — transações por bairro",
                data=df_bairro,
                columns=["BAIRRO", "TOTAL_TRANSACOES"],
                key_on=f"feature.properties.{choropleth_key}",
                fill_color="YlOrRd",
                fill_opacity=0.65,
                line_opacity=0.4,
                legend_name="Total de transações por bairro",
                show=False,
            ).add_to(mapa)
            log.info("  Choropleth adicionado: %s", geojson_bairros)
        else:
            log.warning(
                "  GeoJSON não encontrado: '%s' — choropleth omitido.",
                geojson_bairros,
            )

    # -----------------------------------------------------------------------
    # Marcadores clicáveis (opcionais)
    # -----------------------------------------------------------------------
    if incluir_marcadores:
        log.info("  Adicionando %d marcadores clicáveis...", len(df))
        for rec in df.to_dict(orient="records"):  # type: ignore[call-overload]
            lat = float(rec["LAT"])  # type: ignore[arg-type]
            lon = float(rec["LON"])  # type: ignore[arg-type]
            val_raw = rec.get(col_valor) if col_valor else None
            # NaN check sem importar math
            val_ok = val_raw is not None and val_raw == val_raw
            val_str = (
                f"R$ {float(val_raw):,.0f}".replace(",", "X")  # type: ignore[arg-type]
                .replace(".", ",")
                .replace("X", ".")
                if val_ok
                else "N/D"
            )
            popup_html = (
                f'<div style="font-family:Arial;font-size:13px;min-width:200px">'
                f"<b>{rec.get('NOME DO LOGRADOURO', '?')}</b><br>"
                f"<i>{rec.get('BAIRRO', '?')}</i><br><br>"
                f"<b>Ano:</b> {rec.get('ANO DO PAGAMENTO DO ITBI', '?')}<br>"
                f"<b>Tipologia:</b> {rec.get('PRINCIPAL TIPOLOGIA', '?')}<br>"
                f"<b>Natureza:</b> {rec.get('PRINCIPAL NATUREZA DA TRANSAÇÃO', '?')}<br>"
                f"<b>Transações:</b> {rec.get(col_qtd, '?') if col_qtd else '?'}<br>"
                f"<b>Valor médio:</b> {val_str}"
                f"</div>"
            )
            folium.CircleMarker(
                location=[lat, lon],
                radius=4,
                color="#2563eb",
                fill=True,
                fill_opacity=0.5,
                popup=folium.Popup(popup_html, max_width=280),
                tooltip=f"{rec.get('NOME DO LOGRADOURO', '?')} — {val_str}",
            ).add_to(mapa)
    else:
        log.info("  Marcadores omitidos (incluir_marcadores=False).")

    folium.LayerControl().add_to(mapa)

    # -----------------------------------------------------------------------
    # Injeta painel de filtros + estatísticas (Fase 4)
    # -----------------------------------------------------------------------
    pontos_js = _construir_pontos_js(df, col_valor, col_qtd)
    controles_html = _construir_controles_filtro(pontos_js)
    # branca.element.Figure tem atributo .html; get_root() retorna Figure
    mapa.get_root().html.add_child(Element(controles_html))  # type: ignore[union-attr]
    log.info("  Painel de filtros injetado (%d pontos para JS).", len(df))

    mapa.save(str(output_path))
    log.info("  Heatmap salvo: %s", output_path)

    # -----------------------------------------------------------------------
    # Exporta JSON para GitHub Pages (inclui ano e quantidade — Fase 4)
    # -----------------------------------------------------------------------
    colunas_json = [c for c in _COLUNAS_JSON_BASE if c in df.columns]

    col_ano = _detect_col(df, "ANO", "PAGAMENTO")
    if col_ano and col_ano not in colunas_json:
        colunas_json.append(col_ano)
    if col_qtd and col_qtd not in colunas_json:
        colunas_json.append(col_qtd)

    records: list[dict] = df[colunas_json].to_dict(  # type: ignore[call-overload]
        orient="records"
    )

    if col_valor:
        col_attr = (
            col_valor.replace(" ", "_")
            .replace("(", "")
            .replace(")", "")
            .replace("$", "")
            .replace("/", "")
        )
        for i, row in enumerate(df.itertuples()):
            records[i]["valor_medio"] = _safe_val(getattr(row, col_attr, None))

    json_path.write_text(
        json.dumps(records, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("  JSON exportado: %s", json_path)


# ===========================================================================
# Entrypoint standalone: python -m itbi.heatmap
# ===========================================================================


def _build_arg_parser():  # type: ignore[return]
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m itbi.heatmap",
        description=(
            "[ETAPA 5] Gera heatmap interativo (docs/index.html) "
            "e JSON de dados (docs/data/itbi_geo.json). "
            "Inclui filtros client-side por ano e bairro, painel de "
            "estatísticas e suporte a choropleth opcional."
        ),
    )
    parser.add_argument(
        "--no-markers",
        action="store_true",
        help="Omite marcadores clicáveis (mapa mais leve para volumes grandes).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_HTML,
        metavar="PATH",
        help=f"Caminho de saída do HTML (padrão: {OUTPUT_HTML})",
    )
    parser.add_argument(
        "--json-output",
        type=Path,
        default=DATA_JSON,
        metavar="PATH",
        help=f"Caminho de saída do JSON (padrão: {DATA_JSON})",
    )
    parser.add_argument(
        "--origem",
        type=Path,
        default=DATA_DIR / "consolidado_geo.csv",
        metavar="CSV",
        help=f"CSV geocodificado de entrada (padrão: {DATA_DIR}/consolidado_geo.csv)",
    )
    parser.add_argument(
        "--choropleth-geojson",
        type=Path,
        default=None,
        metavar="GEOJSON",
        help=(
            "Caminho para GeoJSON local dos bairros de Niterói. "
            "Quando fornecido, adiciona camada choropleth (toggle via LayerControl)."
        ),
    )
    parser.add_argument(
        "--choropleth-key",
        type=str,
        default="nome",
        metavar="PROP",
        help=(
            "Propriedade GeoJSON para correlacionar nomes de bairros "
            "(padrão: 'nome'). Ex.: 'nome_bairro', 'NOME'."
        ),
    )
    return parser


if __name__ == "__main__":
    import sys

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    args = _build_arg_parser().parse_args()
    origem: Path = args.origem

    if not origem.exists():
        log.error(
            "Arquivo não encontrado: '%s'. "
            "Execute 'python -m itbi.geocodificacao' primeiro.",
            origem,
        )
        sys.exit(1)

    df = pd.read_csv(origem)
    gerar_heatmap(
        df,
        output_path=args.output,
        json_path=args.json_output,
        incluir_marcadores=not args.no_markers,
        geojson_bairros=args.choropleth_geojson,
        choropleth_key=args.choropleth_key,
    )
    print(f"\nHeatmap gerado: {args.output}")
    print(f"JSON exportado: {args.json_output}")
    sys.exit(0)
