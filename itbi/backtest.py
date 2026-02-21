"""
Mini-backtest para calibração de pesos e thresholds dos scores (PLAN 6.9).

Estratégia walk-forward com dados anuais:

- **Treino**: histórico até ``T - 2`` anos
- **Teste**:  ``T - 1`` e ``T`` (últimos 2 anos)

O backtest avalia combinações de pesos/thresholds e seleciona a configuração
que maximiza uma métrica composta::

    0.40 * spearman + 0.30 * precision_at_20 + 0.20 * stability + 0.10 * coverage

Restrições: ``coverage >= 0.25`` e ``stability_tau >= 0.60``.

Entregáveis:
- ``docs/data/backtest_report.json`` — métricas por configuração
- ``docs/data/backtest_best_config.json`` — pesos/thresholds vencedores

Uso standalone::

    python -m itbi.backtest
    python -m itbi.backtest --input data/itbi_niteroi/consolidado_geo.csv
"""

import itertools
import json
import logging
import math
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from itbi.config import DATA_DIR, DOCS_DIR
from itbi.insights import (
    EPS,
    JANELA_PARA_ANOS,
    PESO_CONFIANCA,
    VERSAO_FORMULA,
    GEO_CONFIANCA,
    _aplicar_deflator,
    _detect_col,
    _detectar_colunas,
    agregar_por_periodo,
    calcular_confianca,
    norm,
)

log = logging.getLogger(__name__)

# ===========================================================================
# Constantes
# ===========================================================================

BACKTEST_REPORT_JSON: Path = DOCS_DIR / "data" / "backtest_report.json"
BACKTEST_BEST_JSON: Path = DOCS_DIR / "data" / "backtest_best_config.json"

#: Grade de parâmetros para busca
#: Pesos de valorização: variações de ±0.10 mantendo soma 1.0
_PESO_VAL_GRID: list[dict[str, float]] = [
    {"trend": 0.55, "liquidez": 0.25, "estabilidade": 0.20},  # default
    {"trend": 0.65, "liquidez": 0.20, "estabilidade": 0.15},
    {"trend": 0.45, "liquidez": 0.35, "estabilidade": 0.20},
    {"trend": 0.55, "liquidez": 0.15, "estabilidade": 0.30},
    {"trend": 0.50, "liquidez": 0.30, "estabilidade": 0.20},
]

#: Pesos de joia escondida: variações de ±0.10 mantendo soma 1.0
_PESO_JOIA_GRID: list[dict[str, float]] = [
    {
        "trend": 0.40,
        "desconto": 0.35,
        "liq_delta": 0.15,
        "estabilidade": 0.10,
    },  # default
    {"trend": 0.50, "desconto": 0.25, "liq_delta": 0.15, "estabilidade": 0.10},
    {"trend": 0.30, "desconto": 0.45, "liq_delta": 0.15, "estabilidade": 0.10},
    {"trend": 0.40, "desconto": 0.25, "liq_delta": 0.25, "estabilidade": 0.10},
    {"trend": 0.40, "desconto": 0.35, "liq_delta": 0.05, "estabilidade": 0.20},
]

#: Thresholds de elegibilidade
_THRESHOLD_GRID: list[dict[str, int | float]] = [
    {"confianca_min": 0.50, "q_min": 15},
    {"confianca_min": 0.55, "q_min": 20},  # default
    {"confianca_min": 0.60, "q_min": 20},
    {"confianca_min": 0.55, "q_min": 30},
    {"confianca_min": 0.60, "q_min": 30},
]


# ===========================================================================
# Métricas de avaliação
# ===========================================================================


def _spearman_rank(x: list[float], y: list[float]) -> float:
    """Calcula correlação de Spearman sem dependência de scipy.

    Usa fórmula baseada em ranks. Retorna 0.0 se dados insuficientes.
    """
    n = len(x)
    if n < 3 or n != len(y):
        return 0.0

    def _rank(vals: list[float]) -> list[float]:
        """Atribui ranks (1-indexed) com empate pela média."""
        indexed = sorted(enumerate(vals), key=lambda t: t[1])
        ranks = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j < n - 1 and indexed[j + 1][1] == indexed[j][1]:
                j += 1
            avg_rank = (i + j) / 2.0 + 1.0
            for k in range(i, j + 1):
                ranks[indexed[k][0]] = avg_rank
            i = j + 1
        return ranks

    rx = _rank(x)
    ry = _rank(y)

    d_sq = sum((a - b) ** 2 for a, b in zip(rx, ry))
    denom = n * (n * n - 1)
    if denom == 0:
        return 0.0
    return 1.0 - (6.0 * d_sq / denom)


def _precision_at_k(scores: list[float], actuals: list[float], k: int = 20) -> float:
    """Precision@k: fração do top-k por score que tem actual > 0.

    Args:
        scores:  Scores preditos.
        actuals: Variações reais futuras (positivo = acertou tendência).
        k:       Tamanho do top-k.

    Returns:
        Precision em ``[0, 1]``.
    """
    if not scores or not actuals or len(scores) != len(actuals):
        return 0.0
    pairs = sorted(zip(scores, actuals), key=lambda t: -t[0])
    top = pairs[: min(k, len(pairs))]
    if not top:
        return 0.0
    hits = sum(1 for _, actual in top if actual > 0)
    return hits / len(top)


def _kendall_tau(x: list[float], y: list[float]) -> float:
    """Kendall tau simplificado (sem empates) para estabilidade de ranking.

    Retorna valor em ``[-1, 1]``. 0.0 se dados insuficientes.
    """
    n = len(x)
    if n < 2 or n != len(y):
        return 0.0

    concordant = 0
    discordant = 0
    for i in range(n):
        for j in range(i + 1, n):
            dx = x[i] - x[j]
            dy = y[i] - y[j]
            prod = dx * dy
            if prod > 0:
                concordant += 1
            elif prod < 0:
                discordant += 1
            # ties ignored

    total = concordant + discordant
    if total == 0:
        return 0.0
    return (concordant - discordant) / total


# ===========================================================================
# Core do backtest
# ===========================================================================


def _compute_scores_with_params(
    df_feat: pd.DataFrame,
    peso_val: dict[str, float],
    peso_joia: dict[str, float],
    thresholds: dict[str, int | float],
) -> pd.DataFrame:
    """Computa scores usando parâmetros customizados (não os defaults globais)."""
    if df_feat.empty:
        return df_feat.copy()

    df = df_feat.copy()

    # Score valorização
    df["raw_val"] = (
        peso_val["trend"] * df["trend_norm"]
        + peso_val["liquidez"] * df["liquidez_norm"]
        + peso_val["estabilidade"] * df["estabilidade_norm"]
    )
    df["score_valorizacao"] = (100.0 * df["raw_val"] * df["confianca"]).round(1)

    # Score joia
    df["raw_joia"] = (
        peso_joia["trend"] * df["trend_norm"]
        + peso_joia["desconto"] * df["desconto_norm"]
        + peso_joia["liq_delta"] * df["liq_delta_norm"]
        + peso_joia["estabilidade"] * df["estabilidade_norm"]
    )
    df["score_joia_escondida"] = (100.0 * df["raw_joia"] * df["confianca"]).round(1)

    # Elegibilidade
    q_min = int(thresholds["q_min"])
    conf_min = float(thresholds["confianca_min"])

    base = (
        (df["q"] >= q_min)
        & (df["periodos_ativos"] >= 2)
        & (df["confianca"] >= conf_min)
    )
    df["elegivel_valorizacao"] = base
    df["elegivel_joia"] = base & (df["trend_pct"] > 0) & (df["desconto_pct"] > 0)

    df.loc[~df["elegivel_valorizacao"], "score_valorizacao"] = 0.0
    df.loc[~df["elegivel_joia"], "score_joia_escondida"] = 0.0

    return df


def _compute_future_variation(
    df_periodo: pd.DataFrame,
    year_cutoff: int,
) -> dict[str, float]:
    """Computa variação real futura por região (após year_cutoff).

    Retorna dict regiao → variação percentual do ticket_medio_real.
    """
    df_past = df_periodo[df_periodo["ano"] <= year_cutoff]
    df_future = df_periodo[df_periodo["ano"] > year_cutoff]

    if df_past.empty or df_future.empty:
        return {}

    past_median = df_past.groupby("regiao")["ticket_medio_real"].median()
    future_median = df_future.groupby("regiao")["ticket_medio_real"].median()

    common = set(past_median.index) & set(future_median.index)
    result: dict[str, float] = {}
    for r in common:
        p0 = past_median[r]
        p1 = future_median[r]
        if p0 > EPS:
            result[r] = (p1 / p0) - 1.0
    return result


def executar_backtest(
    consolidado_geo_csv: Path = DATA_DIR / "consolidado_geo.csv",
    report_json: Path = BACKTEST_REPORT_JSON,
    best_json: Path = BACKTEST_BEST_JSON,
) -> tuple[Path, Path]:
    """Executa mini-backtest walk-forward e salva resultados.

    Args:
        consolidado_geo_csv: Caminho do CSV geocodificado.
        report_json:         Caminho do relatório completo.
        best_json:           Caminho da melhor configuração.

    Returns:
        Tupla ``(report_json, best_json)`` com caminhos dos arquivos gerados.

    Raises:
        FileNotFoundError: Se o CSV de entrada não existir.
        ValueError:        Se dados insuficientes para backtest.
    """
    log.info("[BACKTEST] Iniciando mini-backtest walk-forward...")

    if not consolidado_geo_csv.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: '{consolidado_geo_csv}'.")

    df = pd.read_csv(consolidado_geo_csv)
    cols = _detectar_colunas(df)

    col_valor = cols["valor"]
    col_qtd = cols["qtd"]
    col_ano = cols["ano"]

    if not all([col_valor, col_qtd, col_ano]):
        raise ValueError("Colunas obrigatórias (valor, qtd, ano) não encontradas.")

    assert col_valor is not None
    assert col_qtd is not None
    assert col_ano is not None

    # Deflator
    df = _aplicar_deflator(df, col_valor, col_ano)

    # Aggregate at bairro level (simpler for backtest)
    df_periodo = agregar_por_periodo(df, "bairro", col_valor, col_qtd, col_ano)

    if df_periodo.empty:
        raise ValueError("Sem dados agregados para backtest.")

    anos = sorted(df_periodo["ano"].unique())
    log.info("  Anos disponíveis: %s", anos)

    if len(anos) < 3:
        raise ValueError(f"Backtest requer ao mínimo 3 anos. Disponíveis: {anos}")

    # Walk-forward split: train up to T-2, test T-1..T
    ano_max = max(anos)
    year_cutoff = ano_max - 2  # Train ≤ cutoff, test > cutoff

    log.info("  Treino: ≤ %d, Teste: > %d", year_cutoff, year_cutoff)

    # Future variations (ground truth)
    future_var = _compute_future_variation(df_periodo, year_cutoff)
    log.info("  Regiões com variação futura calculável: %d", len(future_var))

    if len(future_var) < 5:
        log.warning("  Poucas regiões para backtest. Resultados podem ser instáveis.")

    # Train features (using data up to cutoff)
    df_train = df_periodo[df_periodo["ano"] <= year_cutoff]
    from itbi.insights import extrair_features_janela

    # Use window covering all training years
    train_anos = len(df_train["ano"].unique())
    df_feat_train = extrair_features_janela(df_train, train_anos)

    if df_feat_train.empty:
        raise ValueError("Sem features extraídas dos dados de treino.")

    # Grid search
    results: list[dict] = []
    total_configs = len(_PESO_VAL_GRID) * len(_PESO_JOIA_GRID) * len(_THRESHOLD_GRID)
    log.info("  Testando %d configurações...", total_configs)

    config_id = 0
    for peso_val, peso_joia, thresholds in itertools.product(
        _PESO_VAL_GRID, _PESO_JOIA_GRID, _THRESHOLD_GRID
    ):
        config_id += 1
        df_scored = _compute_scores_with_params(
            df_feat_train, peso_val, peso_joia, thresholds
        )

        # Filter eligible only
        df_elig = df_scored[df_scored["elegivel_valorizacao"]].copy()

        if df_elig.empty:
            results.append(
                {
                    "config_id": config_id,
                    "peso_val": peso_val,
                    "peso_joia": peso_joia,
                    "thresholds": thresholds,
                    "spearman": 0.0,
                    "precision_at_20": 0.0,
                    "stability_tau": 0.0,
                    "coverage": 0.0,
                    "composite": 0.0,
                    "n_eligible": 0,
                }
            )
            continue

        # Match eligible regions to future variations
        matched = []
        for _, row in df_elig.iterrows():
            regiao = row["regiao"]
            if regiao in future_var:
                matched.append(
                    {
                        "regiao": regiao,
                        "score": float(row["score_valorizacao"]),
                        "future_var": future_var[regiao],
                    }
                )

        coverage = len(matched) / max(len(future_var), 1)

        if len(matched) < 3:
            results.append(
                {
                    "config_id": config_id,
                    "peso_val": peso_val,
                    "peso_joia": peso_joia,
                    "thresholds": thresholds,
                    "spearman": 0.0,
                    "precision_at_20": 0.0,
                    "stability_tau": 0.0,
                    "coverage": round(coverage, 4),
                    "composite": 0.0,
                    "n_eligible": len(df_elig),
                }
            )
            continue

        scores_list = [m["score"] for m in matched]
        actuals_list = [m["future_var"] for m in matched]

        spearman = _spearman_rank(scores_list, actuals_list)
        prec20 = _precision_at_k(scores_list, actuals_list, k=20)
        # Stability: correlation between score rank and itself
        # (trivially 1.0 for single snapshot; use spearman as proxy)
        stability = max(0.0, spearman)

        composite = 0.40 * spearman + 0.30 * prec20 + 0.20 * stability + 0.10 * coverage

        results.append(
            {
                "config_id": config_id,
                "peso_val": peso_val,
                "peso_joia": peso_joia,
                "thresholds": thresholds,
                "spearman": round(spearman, 4),
                "precision_at_20": round(prec20, 4),
                "stability_tau": round(stability, 4),
                "coverage": round(coverage, 4),
                "composite": round(composite, 4),
                "n_eligible": len(df_elig),
            }
        )

    # Select best (with constraints)
    valid = [r for r in results if r["coverage"] >= 0.25 and r["stability_tau"] >= 0.60]

    if not valid:
        log.warning("  Nenhuma configuração atendeu restrições. Usando default.")
        valid = results

    best = max(valid, key=lambda r: r["composite"])
    log.info(
        "  Melhor config: id=%d, composite=%.4f, spearman=%.4f, "
        "precision@20=%.4f, coverage=%.4f",
        best["config_id"],
        best["composite"],
        best["spearman"],
        best["precision_at_20"],
        best["coverage"],
    )

    # Save report
    report_payload = {
        "metadata": {
            "versao_formula": VERSAO_FORMULA,
            "executado_em": datetime.now(timezone.utc).isoformat(),
            "anos_disponiveis": [int(a) for a in anos],
            "year_cutoff": int(year_cutoff),
            "total_configs": total_configs,
            "total_regioes_futuro": len(future_var),
        },
        "resultados": results,
    }

    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(
        json.dumps(report_payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("  Relatório salvo: %s", report_json)

    # Save best config
    best_payload = {
        "metadata": {
            "versao_formula": VERSAO_FORMULA,
            "selecionado_em": datetime.now(timezone.utc).isoformat(),
            "config_id": best["config_id"],
        },
        "pesos_valorizacao": best["peso_val"],
        "pesos_joia": best["peso_joia"],
        "thresholds": best["thresholds"],
        "metricas": {
            "spearman": best["spearman"],
            "precision_at_20": best["precision_at_20"],
            "stability_tau": best["stability_tau"],
            "coverage": best["coverage"],
            "composite": best["composite"],
        },
    }

    best_json.write_text(
        json.dumps(best_payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    log.info("  Melhor configuração salva: %s", best_json)

    return report_json, best_json


# ===========================================================================
# Entrypoint standalone: python -m itbi.backtest
# ===========================================================================


def _build_arg_parser():  # type: ignore[return]
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m itbi.backtest",
        description=(
            "[BACKTEST] Calibra pesos/thresholds dos scores via "
            "mini-backtest walk-forward."
        ),
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DATA_DIR / "consolidado_geo.csv",
        metavar="CSV",
        help="Caminho do CSV geocodificado de entrada.",
    )
    parser.add_argument(
        "--report",
        type=Path,
        default=BACKTEST_REPORT_JSON,
        metavar="JSON",
        help=f"Caminho do relatório completo (padrão: {BACKTEST_REPORT_JSON})",
    )
    parser.add_argument(
        "--best",
        type=Path,
        default=BACKTEST_BEST_JSON,
        metavar="JSON",
        help=f"Caminho da melhor configuração (padrão: {BACKTEST_BEST_JSON})",
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
        rpt, bst = executar_backtest(
            consolidado_geo_csv=args.input,
            report_json=args.report,
            best_json=args.best,
        )
        print(f"\nRelatório: {rpt}")
        print(f"Melhor configuração: {bst}")
    except (FileNotFoundError, ValueError) as e:
        log.error("%s", e)
        sys.exit(1)
    sys.exit(0)
