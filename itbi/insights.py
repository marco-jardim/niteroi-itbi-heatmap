"""
Etapa 6 — Inteligência de valorização e "joias escondidas".

Implementa a fórmula v0.1 documentada em PLAN.md (seção 6.3):

* ``score_valorizacao`` (0–100): tendência de preço + liquidez + estabilidade
* ``score_joia_escondida`` (0–100): desconto vs benchmark + tendência + liquidez delta

**Adaptação para dados anuais:**

Os dados públicos do ITBI Niterói são anuais (médias por logradouro/ano).
A fórmula v0.1 do PLAN usa granularidade mensal; esta implementação adapta
para períodos anuais:

- Janela 12m → 2 anos mais recentes (mínimo para calcular tendência)
- Janela 24m → 3 anos mais recentes
- Janela 36m → todos os anos disponíveis

Uso standalone::

    python -m itbi.insights
    python -m itbi.insights --input data/itbi_niteroi/consolidado_geo.csv
"""

import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from itbi.config import DATA_DIR, DOCS_DIR

log = logging.getLogger(__name__)

# ===========================================================================
# Constantes
# ===========================================================================

EPS: float = 1e-9

#: Caminho do JSON de insights gerado
INSIGHTS_JSON: Path = DOCS_DIR / "data" / "itbi_insights.json"

#: Deflator IPCA anual — fator multiplicativo para converter valor nominal
#: em valor real base dez/2024.
#: Fonte: IBGE/SIDRA série 1737. Calculado como IPCA_dez2024 / IPCA_dezAno.
DEFLATOR_IPCA: dict[int, float] = {
    2020: 1.278,
    2021: 1.161,
    2022: 1.098,
    2023: 1.049,
    2024: 1.000,
}

# --- Mapeamento de janelas (meses PLAN → anos efetivos) ---
JANELA_PARA_ANOS: dict[int, int] = {12: 2, 24: 3, 36: 5}

# --- Elegibilidade ---
MIN_TRANSACOES: int = 20
MIN_PERIODOS_ATIVOS: int = 2
MIN_CONFIANCA: float = 0.55

# --- Pesos do score de valorização ---
PESO_VALORIZACAO: dict[str, float] = {
    "trend": 0.55,
    "liquidez": 0.25,
    "estabilidade": 0.20,
}

# --- Pesos do score joia escondida ---
PESO_JOIA: dict[str, float] = {
    "trend": 0.40,
    "desconto": 0.35,
    "liq_delta": 0.15,
    "estabilidade": 0.10,
}

# --- Pesos de confiança ---
PESO_CONFIANCA: dict[str, float] = {
    "amostra": 0.50,
    "cobertura": 0.30,
    "geo": 0.20,
}

GEO_CONFIANCA: dict[str, float] = {
    "endereco": 1.0,
    "bairro": 0.7,
    "centroide": 0.4,
}

#: Versão da fórmula (rastreabilidade no JSON)
VERSAO_FORMULA: str = "v0.1"


# ===========================================================================
# Funções utilitárias puras
# ===========================================================================


def norm(x: float, lo: float, hi: float) -> float:
    """Normaliza *x* para ``[0, 1]`` dado intervalo ``[lo, hi]``.

    Valores fora do intervalo são clampados (clip).

    Args:
        x:  Valor a normalizar.
        lo: Limite inferior (mapeado para 0).
        hi: Limite superior (mapeado para 1).

    Returns:
        Valor normalizado em ``[0.0, 1.0]``.
    """
    if hi <= lo:
        return 0.0
    x_clip = min(max(x, lo), hi)
    return (x_clip - lo) / (hi - lo)


def selo_confianca(confianca: float) -> str:
    """Retorna selo textual de confiança conforme PLAN v0.1.

    Args:
        confianca: Valor numérico de confiança (0–1).

    Returns:
        ``"alta"`` (≥ 0.75), ``"media"`` (0.55–0.74) ou ``"baixa"`` (< 0.55).
    """
    if confianca >= 0.75:
        return "alta"
    if confianca >= 0.55:
        return "media"
    return "baixa"


def calcular_confianca(
    q: int,
    periodos_ativos: int,
    periodos_janela: int,
    nivel_geo: str,
) -> float:
    """Calcula confiança composta do insight.

    Combina três componentes com pesos de :data:`PESO_CONFIANCA`:

    - ``c_amostra``: proporção de transações vs. limiar 30.
    - ``c_cobertura``: fração de períodos ativos na janela.
    - ``c_geo``: qualidade do geocoding (endereço > bairro > centroide).

    Args:
        q:                Total de transações na janela.
        periodos_ativos:  Número de períodos (anos) com dados na janela.
        periodos_janela:  Tamanho total da janela em períodos.
        nivel_geo:        Nível de geocodificação predominante.

    Returns:
        Valor de confiança em ``[0, 1]``.
    """
    c_amostra = min(1.0, q / 30)
    c_cobertura = periodos_ativos / max(periodos_janela, 1)
    c_geo = GEO_CONFIANCA.get(nivel_geo, 0.4)
    return (
        PESO_CONFIANCA["amostra"] * c_amostra
        + PESO_CONFIANCA["cobertura"] * c_cobertura
        + PESO_CONFIANCA["geo"] * c_geo
    )


# ===========================================================================
# Detecção de colunas
# ===========================================================================


def _detect_col(df: pd.DataFrame, *fragments: str) -> str | None:
    """Retorna primeira coluna cujo nome contém todos os fragmentos."""
    for col in df.columns:
        if all(f in col for f in fragments):
            return col
    return None


def _detectar_colunas(df: pd.DataFrame) -> dict[str, str | None]:
    """Detecta colunas relevantes no DataFrame consolidado.

    Returns:
        Dict com chaves ``valor``, ``qtd``, ``ano``, ``bairro``,
        ``logradouro``, ``nivel_geo`` mapeadas para nomes reais.
    """
    return {
        "valor": _detect_col(df, "VALOR DA TRANSA")
        or _detect_col(df, "VALOR DE AVALIA"),
        "qtd": _detect_col(df, "QUANTIDADE"),
        "ano": _detect_col(df, "ANO", "PAGAMENTO") or _detect_col(df, "ANO"),
        "bairro": "BAIRRO" if "BAIRRO" in df.columns else None,
        "logradouro": (
            "NOME DO LOGRADOURO" if "NOME DO LOGRADOURO" in df.columns else None
        ),
        "nivel_geo": "NIVEL_GEO" if "NIVEL_GEO" in df.columns else None,
    }


# ===========================================================================
# Deflator IPCA
# ===========================================================================


def _aplicar_deflator(df: pd.DataFrame, col_valor: str, col_ano: str) -> pd.DataFrame:
    """Cria coluna ``VALOR_REAL`` aplicando deflator IPCA ao valor nominal.

    Anos sem deflator configurado usam fator 1.0 (valor nominal).

    Args:
        df:        DataFrame com colunas de valor e ano.
        col_valor: Nome da coluna de valor nominal.
        col_ano:   Nome da coluna de ano.

    Returns:
        DataFrame com coluna ``VALOR_REAL`` adicionada.
    """
    df = df.copy()
    df["_ANO_INT"] = pd.to_numeric(df[col_ano], errors="coerce").astype("Int64")
    df["_DEFLATOR"] = df["_ANO_INT"].map(DEFLATOR_IPCA).fillna(1.0)
    df["VALOR_REAL"] = df[col_valor] * df["_DEFLATOR"]
    df.drop(columns=["_ANO_INT", "_DEFLATOR"], inplace=True)
    return df


# ===========================================================================
# Agregação por período
# ===========================================================================


def agregar_por_periodo(
    df: pd.DataFrame,
    nivel: str,
    col_valor: str,
    col_qtd: str,
    col_ano: str,
) -> pd.DataFrame:
    """Agrega dados por região e período (ano).

    Para ``nivel="logradouro"``, agrupa por ``[BAIRRO, NOME DO LOGRADOURO, ano]``.
    Para ``nivel="bairro"``, agrupa por ``[BAIRRO, ano]``.

    Returns:
        DataFrame com colunas: ``regiao``, ``bairro``, ``ano``, ``qtd``,
        ``valor_total_real``, ``ticket_medio_real``, ``nivel_geo_predominante``.
    """
    df = df.copy()
    df["_ANO"] = pd.to_numeric(df[col_ano], errors="coerce")
    df["_QTD"] = pd.to_numeric(df[col_qtd], errors="coerce").fillna(0)
    df["_VALOR_REAL"] = pd.to_numeric(df["VALOR_REAL"], errors="coerce").fillna(0)

    if nivel == "logradouro":
        group_cols = ["BAIRRO", "NOME DO LOGRADOURO", "_ANO"]
    else:
        group_cols = ["BAIRRO", "_ANO"]

    # Filtra linhas com ano válido
    mask = df["_ANO"].notna()
    df_valid = df[mask]

    agg_dict: dict[str, tuple[str, str]] = {
        "qtd": ("_QTD", "sum"),
        "valor_total_real": ("_VALOR_REAL", "sum"),
    }

    # Nível geo predominante: moda do NIVEL_GEO no grupo
    if "NIVEL_GEO" in df_valid.columns:
        # Use first after sorting by frequency
        def _predominante(s: pd.Series) -> str:
            if s.empty:
                return "centroide"
            counts = s.value_counts()
            return str(counts.index[0]) if len(counts) > 0 else "centroide"

        grouped = df_valid.groupby(group_cols, dropna=False)
        result = grouped.agg(**agg_dict).reset_index()
        nivel_geo_series = grouped["NIVEL_GEO"].agg(_predominante).reset_index()
        nivel_geo_series.columns = [*group_cols, "nivel_geo_predominante"]
        result = result.merge(nivel_geo_series, on=group_cols, how="left")
    else:
        result = (
            df_valid.groupby(group_cols, dropna=False).agg(**agg_dict).reset_index()
        )
        result["nivel_geo_predominante"] = "centroide"

    # Ticket médio
    result["ticket_medio_real"] = result["valor_total_real"] / result["qtd"].clip(
        lower=1
    )

    # Normalizar nomes
    result.rename(columns={"_ANO": "ano"}, inplace=True)
    result["ano"] = result["ano"].astype(int)

    if nivel == "logradouro":
        result["regiao"] = (
            result["NOME DO LOGRADOURO"].astype(str)
            + " — "
            + result["BAIRRO"].astype(str)
        )
        result["bairro"] = result["BAIRRO"]
    else:
        result["regiao"] = result["BAIRRO"].astype(str)
        result["bairro"] = result["BAIRRO"]

    cols_out = [
        "regiao",
        "bairro",
        "ano",
        "qtd",
        "valor_total_real",
        "ticket_medio_real",
        "nivel_geo_predominante",
    ]
    return result[[c for c in cols_out if c in result.columns]].copy()


# ===========================================================================
# Extração de features por janela
# ===========================================================================


def extrair_features_janela(
    df_periodo: pd.DataFrame,
    anos_janela: int,
    df_benchmark: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Extrai features por região dentro de uma janela temporal.

    Para cada região calcula:
    - ``trend_pct``, ``trend_norm``
    - ``q`` (total transações), ``liquidez_norm``
    - ``cv`` (coeficiente de variação), ``estabilidade_norm``
    - ``desconto_pct``, ``desconto_norm`` (vs benchmark)
    - ``liq_delta_pct``, ``liq_delta_norm``
    - ``periodos_ativos``
    - ``confianca``, ``selo``

    Args:
        df_periodo:    Saída de :func:`agregar_por_periodo`.
        anos_janela:   Quantos anos recentes considerar.
        df_benchmark:  DataFrame de benchmark (nível superior) para desconto.
                       Se ``None``, desconto é 0.

    Returns:
        DataFrame com uma linha por região e colunas de features.
    """
    if df_periodo.empty:
        return pd.DataFrame()

    ano_max = int(df_periodo["ano"].max())
    ano_min_janela = ano_max - anos_janela + 1
    df_w = df_periodo[df_periodo["ano"] >= ano_min_janela].copy()

    if df_w.empty:
        return pd.DataFrame()

    # Preparar benchmark lookup: bairro → ticket_medio_real mediano
    bench_lookup: dict[str, float] = {}
    if df_benchmark is not None and not df_benchmark.empty:
        # Filtrar mesmo janela no benchmark
        df_b = df_benchmark[df_benchmark["ano"] >= ano_min_janela]
        if not df_b.empty:
            bench_agg = df_b.groupby("regiao")["ticket_medio_real"].median()
            bench_lookup = bench_agg.to_dict()

    # Mediana global do benchmark para nível bairro (benchmark = cidade)
    global_median = 0.0
    if df_benchmark is not None and not df_benchmark.empty:
        df_b = df_benchmark[df_benchmark["ano"] >= ano_min_janela]
        if not df_b.empty:
            global_median = float(df_b["ticket_medio_real"].median())

    records: list[dict] = []

    for regiao, grp in df_w.groupby("regiao", dropna=False):
        grp_sorted = grp.sort_values("ano")
        anos_ativos = sorted(grp_sorted["ano"].unique())
        periodos_ativos = len(anos_ativos)

        # --- p0 / p1 (primeiro / último período) ---
        p0 = float(grp_sorted.iloc[0]["ticket_medio_real"])
        p1 = float(grp_sorted.iloc[-1]["ticket_medio_real"])

        # --- Tendência ---
        trend_pct = (p1 / max(p0, EPS)) - 1.0
        trend_norm_val = norm(trend_pct, -0.20, 0.30)

        # --- Liquidez ---
        q = int(grp_sorted["qtd"].sum())
        liquidez_norm_val = min(1.0, math.log1p(q) / math.log1p(120))

        # --- Estabilidade (CV do ticket médio entre períodos) ---
        tickets = grp_sorted["ticket_medio_real"].values
        mean_ticket = float(tickets.mean()) if len(tickets) > 0 else 0.0
        std_ticket = float(tickets.std(ddof=0)) if len(tickets) > 1 else 0.0
        cv = std_ticket / max(mean_ticket, EPS)
        estabilidade_norm_val = 1.0 - min(cv / 0.35, 1.0)

        # --- Nível geo predominante ---
        nivel_geo = (
            str(grp_sorted["nivel_geo_predominante"].mode().iloc[0])
            if not grp_sorted["nivel_geo_predominante"].empty
            else "centroide"
        )

        # --- Confiança ---
        confianca = calcular_confianca(q, periodos_ativos, anos_janela, nivel_geo)

        # --- Desconto vs benchmark ---
        bairro = str(grp_sorted.iloc[0].get("bairro", ""))

        # Determine benchmark reference
        preco_ref = 0.0
        if bench_lookup:
            # logradouro level: benchmark = bairro
            preco_ref = bench_lookup.get(bairro, global_median)
        elif global_median > 0:
            # bairro level: benchmark = city median
            preco_ref = global_median

        desconto_pct = (preco_ref - p1) / max(preco_ref, EPS) if preco_ref > 0 else 0.0
        desconto_norm_val = norm(desconto_pct, 0.00, 0.25)

        # --- Variação de liquidez (split da janela em 2 metades) ---
        mid_year = ano_min_janela + anos_janela // 2
        q_prev = int(grp_sorted[grp_sorted["ano"] < mid_year]["qtd"].sum())
        q_last = int(grp_sorted[grp_sorted["ano"] >= mid_year]["qtd"].sum())
        liq_delta_pct = (q_last - q_prev) / max(q_prev, 1)
        liq_delta_norm_val = norm(liq_delta_pct, -0.30, 0.50)

        records.append(
            {
                "regiao": regiao,
                "bairro": bairro,
                "p0": round(p0, 2),
                "p1": round(p1, 2),
                "trend_pct": round(trend_pct, 4),
                "trend_norm": round(trend_norm_val, 4),
                "q": q,
                "liquidez_norm": round(liquidez_norm_val, 4),
                "cv": round(cv, 4),
                "estabilidade_norm": round(estabilidade_norm_val, 4),
                "periodos_ativos": periodos_ativos,
                "nivel_geo": nivel_geo,
                "confianca": round(confianca, 4),
                "selo": selo_confianca(confianca),
                "preco_ref": round(preco_ref, 2),
                "desconto_pct": round(desconto_pct, 4),
                "desconto_norm": round(desconto_norm_val, 4),
                "liq_delta_pct": round(liq_delta_pct, 4),
                "liq_delta_norm": round(liq_delta_norm_val, 4),
            }
        )

    return pd.DataFrame(records)


# ===========================================================================
# Cálculo dos scores com elegibilidade
# ===========================================================================


def calcular_scores(df_feat: pd.DataFrame) -> pd.DataFrame:
    """Calcula ``score_valorizacao`` e ``score_joia_escondida`` com elegibilidade.

    Regras de elegibilidade (PLAN v0.1):
    - ``q >= 20``, ``periodos_ativos >= 2``, ``confianca >= 0.55``
    - Para joia: ``trend_pct > 0`` **e** ``desconto_pct > 0``

    Args:
        df_feat: DataFrame de features (saída de :func:`extrair_features_janela`).

    Returns:
        DataFrame com colunas ``score_valorizacao``, ``score_joia_escondida``,
        ``elegivel_valorizacao``, ``elegivel_joia`` adicionadas.
    """
    if df_feat.empty:
        return df_feat.copy()

    df = df_feat.copy()

    # --- Score valorização ---
    df["raw_val"] = (
        PESO_VALORIZACAO["trend"] * df["trend_norm"]
        + PESO_VALORIZACAO["liquidez"] * df["liquidez_norm"]
        + PESO_VALORIZACAO["estabilidade"] * df["estabilidade_norm"]
    )
    df["score_valorizacao"] = (100.0 * df["raw_val"] * df["confianca"]).round(1)

    # --- Score joia escondida ---
    df["raw_joia"] = (
        PESO_JOIA["trend"] * df["trend_norm"]
        + PESO_JOIA["desconto"] * df["desconto_norm"]
        + PESO_JOIA["liq_delta"] * df["liq_delta_norm"]
        + PESO_JOIA["estabilidade"] * df["estabilidade_norm"]
    )
    df["score_joia_escondida"] = (100.0 * df["raw_joia"] * df["confianca"]).round(1)

    # --- Elegibilidade ---
    base_elegivel = (
        (df["q"] >= MIN_TRANSACOES)
        & (df["periodos_ativos"] >= MIN_PERIODOS_ATIVOS)
        & (df["confianca"] >= MIN_CONFIANCA)
    )
    df["elegivel_valorizacao"] = base_elegivel

    df["elegivel_joia"] = (
        base_elegivel & (df["trend_pct"] > 0) & (df["desconto_pct"] > 0)
    )

    # Zerar scores de não-elegíveis
    df.loc[~df["elegivel_valorizacao"], "score_valorizacao"] = 0.0
    df.loc[~df["elegivel_joia"], "score_joia_escondida"] = 0.0

    return df


# ===========================================================================
# Orquestrador principal
# ===========================================================================


def gerar_insights(
    consolidado_geo_csv: Path = DATA_DIR / "consolidado_geo.csv",
    output_json: Path = INSIGHTS_JSON,
) -> Path:
    """Gera ``itbi_insights.json`` com scores de valorização e joias escondidas.

    Executa o pipeline completo:
    1. Lê ``consolidado_geo.csv``
    2. Aplica deflator IPCA
    3. Agrega por período para cada nível (bairro, logradouro)
    4. Extrai features por janela (12m, 24m, 36m)
    5. Calcula scores com regras de elegibilidade
    6. Serializa tudo em JSON com metadados

    Args:
        consolidado_geo_csv: Caminho do CSV geocodificado.
        output_json:         Caminho de saída do JSON.

    Returns:
        :class:`~pathlib.Path` do JSON gerado.

    Raises:
        FileNotFoundError: Se o CSV de entrada não existir.
        ValueError:        Se colunas obrigatórias não forem encontradas.
    """
    log.info("[ETAPA 6] Gerando insights de valorização...")

    if not consolidado_geo_csv.exists():
        raise FileNotFoundError(
            f"Arquivo não encontrado: '{consolidado_geo_csv}'. "
            "Execute 'itbi geocodificar' primeiro."
        )

    df = pd.read_csv(consolidado_geo_csv)
    cols = _detectar_colunas(df)

    col_valor = cols["valor"]
    col_qtd = cols["qtd"]
    col_ano = cols["ano"]

    if not col_valor:
        raise ValueError(
            "Coluna de valor da transação não encontrada. "
            f"Colunas disponíveis: {list(df.columns)}"
        )
    if not col_qtd:
        raise ValueError(
            "Coluna de quantidade de transações não encontrada. "
            f"Colunas disponíveis: {list(df.columns)}"
        )
    if not col_ano:
        raise ValueError(
            f"Coluna de ano não encontrada. Colunas disponíveis: {list(df.columns)}"
        )

    log.info(
        "  Colunas detectadas: valor='%s', qtd='%s', ano='%s'",
        col_valor,
        col_qtd,
        col_ano,
    )

    # Deflator
    df = _aplicar_deflator(df, col_valor, col_ano)

    frames: list[pd.DataFrame] = []

    for nivel in ["bairro", "logradouro"]:
        log.info("  Processando nível: %s", nivel)
        df_periodo = agregar_por_periodo(df, nivel, col_valor, col_qtd, col_ano)

        if df_periodo.empty:
            log.warning("  Nenhum dado agregado para nível '%s'.", nivel)
            continue

        # Benchmark: bairro-level aggregation para referência de logradouro
        df_benchmark: pd.DataFrame | None = None
        if nivel == "logradouro":
            df_benchmark = agregar_por_periodo(
                df, "bairro", col_valor, col_qtd, col_ano
            )

        for janela_meses, anos_janela in JANELA_PARA_ANOS.items():
            df_feat = extrair_features_janela(
                df_periodo,
                anos_janela,
                df_benchmark=df_benchmark,
            )
            if df_feat.empty:
                log.warning(
                    "  Sem features para nível='%s', janela=%dm.",
                    nivel,
                    janela_meses,
                )
                continue

            df_scores = calcular_scores(df_feat)
            df_scores["nivel"] = nivel
            df_scores["janela_meses"] = janela_meses
            frames.append(df_scores)
            elegivel_val = df_scores["elegivel_valorizacao"].sum()
            elegivel_joia = df_scores["elegivel_joia"].sum()
            log.info(
                "    janela=%dm: %d regiões, %d elegíveis valorização, %d elegíveis joia",
                janela_meses,
                len(df_scores),
                elegivel_val,
                elegivel_joia,
            )

    if not frames:
        log.warning("  Nenhum insight gerado — dados insuficientes.")
        saida = pd.DataFrame()
    else:
        saida = pd.concat(frames, ignore_index=True)

    # Serializar
    payload: dict = {
        "metadata": {
            "versao_formula": VERSAO_FORMULA,
            "janelas_meses": [12, 24, 36],
            "niveis": ["bairro", "logradouro"],
            "deflator_ipca": DEFLATOR_IPCA,
            "gerado_em": datetime.now(timezone.utc).isoformat(),
            "total_insights": len(saida),
            "total_elegiveis_valorizacao": int(
                saida["elegivel_valorizacao"].sum() if not saida.empty else 0
            ),
            "total_elegiveis_joia": int(
                saida["elegivel_joia"].sum() if not saida.empty else 0
            ),
        },
        "insights": _df_to_records(saida),
    }

    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("  Insights salvos: %s (%d registros)", output_json, len(saida))
    return output_json


def _df_to_records(df: pd.DataFrame) -> list[dict]:
    """Converte DataFrame para lista de dicts com tipos nativos Python.

    Evita problemas de serialização com numpy int64/float64.
    """
    if df.empty:
        return []
    records = df.to_dict(orient="records")
    clean: list[dict] = []
    for rec in records:
        row: dict = {}
        for k, v in rec.items():
            if v is None:
                row[k] = None
            elif isinstance(v, bool):
                row[k] = v
            elif hasattr(v, "item"):
                row[k] = v.item()  # type: ignore[union-attr]
            elif isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                row[k] = None
            else:
                row[k] = v
            # Ensure booleans from numpy are native
            if isinstance(row[k], (int, float)) and k.startswith("elegivel"):
                row[k] = bool(row[k])
        clean.append(row)
    return clean


# ===========================================================================
# Entrypoint standalone: python -m itbi.insights
# ===========================================================================


def _build_arg_parser():  # type: ignore[return]
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m itbi.insights",
        description=(
            "[ETAPA 6] Gera insights de valorização e joias escondidas "
            "a partir do consolidado_geo.csv."
        ),
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DATA_DIR / "consolidado_geo.csv",
        metavar="CSV",
        help=(
            "Caminho do CSV geocodificado de entrada "
            f"(padrão: {DATA_DIR}/consolidado_geo.csv)"
        ),
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=INSIGHTS_JSON,
        metavar="JSON",
        help=f"Caminho de saída do JSON de insights (padrão: {INSIGHTS_JSON})",
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
    try:
        out = gerar_insights(
            consolidado_geo_csv=args.input,
            output_json=args.output,
        )
        print(f"\nInsights gerados: {out}")
    except (FileNotFoundError, ValueError) as e:
        log.error("%s", e)
        sys.exit(1)
    sys.exit(0)
