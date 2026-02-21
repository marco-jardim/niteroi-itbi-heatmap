"""
Testes para itbi.insights e itbi.backtest.

Cobre:
- Funções utilitárias puras: norm, selo_confianca, calcular_confianca
- Reprodutibilidade dos scores (mesmo input → mesmo output)
- Elegibilidade: thresholds de amostra mínima
- Pipeline completo gerar_insights com dados sintéticos
- Backtest com dados sintéticos
"""

import json
from pathlib import Path

import pandas as pd
import pytest

from itbi.insights import (
    EPS,
    MIN_CONFIANCA,
    MIN_PERIODOS_ATIVOS,
    MIN_TRANSACOES,
    agregar_por_periodo,
    calcular_confianca,
    calcular_scores,
    extrair_features_janela,
    gerar_insights,
    norm,
    selo_confianca,
)


# ===========================================================================
# Dados sintéticos (fixture)
# ===========================================================================


def _make_geo_csv(tmp_path: Path, anos: list[int] | None = None) -> Path:
    """Cria consolidado_geo.csv sintético com dados realistas.

    Gera 3 bairros × N anos × 2 logradouros/bairro com valores crescentes
    para simular tendência de valorização.
    """
    if anos is None:
        anos = [2020, 2021, 2022, 2023, 2024]

    rows = []
    bairros = [
        ("Icarai", [("Rua A", 500_000), ("Rua B", 300_000)]),
        ("Centro", [("Rua C", 200_000), ("Rua D", 150_000)]),
        ("Piratininga", [("Rua E", 400_000), ("Rua F", 250_000)]),
    ]

    for ano in anos:
        for bairro, logradouros in bairros:
            for logr, base_valor in logradouros:
                # Simula valorização de ~10% ao ano
                fator = 1 + 0.10 * (ano - anos[0])
                valor = base_valor * fator
                rows.append(
                    {
                        "BAIRRO": bairro,
                        "NOME DO LOGRADOURO": logr,
                        "ANO DO PAGAMENTO DO ITBI": ano,
                        "VALOR DA TRANSAÇÃO (R$)": valor,
                        "QUANTIDADE DE TRANSAÇÕES": 25 + (ano - anos[0]) * 5,
                        "LAT": -22.90 + hash(bairro + logr) % 100 * 0.001,
                        "LON": -43.11 + hash(logr + bairro) % 100 * 0.001,
                        "NIVEL_GEO": "endereco",
                    }
                )

    df = pd.DataFrame(rows)
    csv_path = tmp_path / "consolidado_geo.csv"
    df.to_csv(csv_path, index=False, encoding="utf-8-sig")
    return csv_path


# ===========================================================================
# norm()
# ===========================================================================


class TestNorm:
    def test_valor_no_meio(self) -> None:
        assert norm(0.5, 0.0, 1.0) == pytest.approx(0.5)

    def test_valor_no_minimo(self) -> None:
        assert norm(0.0, 0.0, 1.0) == pytest.approx(0.0)

    def test_valor_no_maximo(self) -> None:
        assert norm(1.0, 0.0, 1.0) == pytest.approx(1.0)

    def test_valor_abaixo_do_minimo_clamp(self) -> None:
        assert norm(-0.5, 0.0, 1.0) == pytest.approx(0.0)

    def test_valor_acima_do_maximo_clamp(self) -> None:
        assert norm(1.5, 0.0, 1.0) == pytest.approx(1.0)

    def test_intervalo_invertido_retorna_zero(self) -> None:
        assert norm(0.5, 1.0, 0.0) == 0.0

    def test_intervalo_igual_retorna_zero(self) -> None:
        assert norm(5.0, 5.0, 5.0) == 0.0

    def test_escala_customizada(self) -> None:
        # norm(0.15, -0.20, 0.30) = (0.15 - (-0.20)) / (0.30 - (-0.20)) = 0.35/0.50 = 0.70
        assert norm(0.15, -0.20, 0.30) == pytest.approx(0.70)


# ===========================================================================
# selo_confianca()
# ===========================================================================


class TestSeloConfianca:
    def test_alta(self) -> None:
        assert selo_confianca(0.75) == "alta"
        assert selo_confianca(0.99) == "alta"

    def test_media(self) -> None:
        assert selo_confianca(0.55) == "media"
        assert selo_confianca(0.74) == "media"

    def test_baixa(self) -> None:
        assert selo_confianca(0.54) == "baixa"
        assert selo_confianca(0.0) == "baixa"


# ===========================================================================
# calcular_confianca()
# ===========================================================================


class TestCalcularConfianca:
    def test_maxima_confianca(self) -> None:
        """q=100, cobertura total, geocoding endereco → max."""
        c = calcular_confianca(
            q=100, periodos_ativos=5, periodos_janela=5, nivel_geo="endereco"
        )
        # 0.5*1.0 + 0.3*1.0 + 0.2*1.0 = 1.0
        assert c == pytest.approx(1.0)

    def test_minima_confianca(self) -> None:
        """q=0, cobertura 0, geocoding centroide → min."""
        c = calcular_confianca(
            q=0, periodos_ativos=0, periodos_janela=5, nivel_geo="centroide"
        )
        # 0.5*0.0 + 0.3*0.0 + 0.2*0.4 = 0.08
        assert c == pytest.approx(0.08)

    def test_confianca_intermediaria(self) -> None:
        c = calcular_confianca(
            q=15, periodos_ativos=3, periodos_janela=5, nivel_geo="bairro"
        )
        # 0.5*(15/30) + 0.3*(3/5) + 0.2*0.7
        # 0.5*0.5 + 0.3*0.6 + 0.2*0.7 = 0.25 + 0.18 + 0.14 = 0.57
        assert c == pytest.approx(0.57)

    def test_geo_desconhecido_usa_centroide(self) -> None:
        c1 = calcular_confianca(
            q=30, periodos_ativos=2, periodos_janela=3, nivel_geo="xyz"
        )
        c2 = calcular_confianca(
            q=30, periodos_ativos=2, periodos_janela=3, nivel_geo="centroide"
        )
        assert c1 == pytest.approx(c2)


# ===========================================================================
# Reprodutibilidade dos scores
# ===========================================================================


def test_scores_reprodutiveis(tmp_path: Path) -> None:
    """Mesmo input deve gerar exatamente os mesmos scores."""
    csv_path = _make_geo_csv(tmp_path)

    out1 = tmp_path / "insights1.json"
    out2 = tmp_path / "insights2.json"

    gerar_insights(consolidado_geo_csv=csv_path, output_json=out1)
    gerar_insights(consolidado_geo_csv=csv_path, output_json=out2)

    data1 = json.loads(out1.read_text(encoding="utf-8"))
    data2 = json.loads(out2.read_text(encoding="utf-8"))

    # Comparar insights (excluir metadata.gerado_em que muda)
    assert len(data1["insights"]) == len(data2["insights"])

    for r1, r2 in zip(data1["insights"], data2["insights"]):
        assert r1["regiao"] == r2["regiao"]
        assert r1["score_valorizacao"] == r2["score_valorizacao"]
        assert r1["score_joia_escondida"] == r2["score_joia_escondida"]
        assert r1["confianca"] == r2["confianca"]


# ===========================================================================
# Elegibilidade
# ===========================================================================


def test_elegibilidade_amostra_insuficiente() -> None:
    """Regiões com q < MIN_TRANSACOES não devem ser elegíveis."""
    df_feat = pd.DataFrame(
        [
            {
                "regiao": "R1",
                "bairro": "B1",
                "p0": 100,
                "p1": 120,
                "trend_pct": 0.2,
                "trend_norm": 0.8,
                "q": 5,  # < MIN_TRANSACOES
                "liquidez_norm": 0.3,
                "cv": 0.1,
                "estabilidade_norm": 0.7,
                "periodos_ativos": 3,
                "nivel_geo": "endereco",
                "confianca": 0.8,
                "selo": "alta",
                "preco_ref": 200,
                "desconto_pct": 0.4,
                "desconto_norm": 1.0,
                "liq_delta_pct": 0.1,
                "liq_delta_norm": 0.5,
            }
        ]
    )

    result = calcular_scores(df_feat)
    assert not result.iloc[0]["elegivel_valorizacao"]
    assert result.iloc[0]["score_valorizacao"] == 0.0


def test_elegibilidade_joia_requer_tendencia_positiva() -> None:
    """Joia escondida requer trend_pct > 0 e desconto_pct > 0."""
    df_feat = pd.DataFrame(
        [
            {
                "regiao": "R1",
                "bairro": "B1",
                "p0": 100,
                "p1": 90,
                "trend_pct": -0.1,  # negativo!
                "trend_norm": 0.2,
                "q": 50,
                "liquidez_norm": 0.8,
                "cv": 0.1,
                "estabilidade_norm": 0.7,
                "periodos_ativos": 4,
                "nivel_geo": "endereco",
                "confianca": 0.9,
                "selo": "alta",
                "preco_ref": 200,
                "desconto_pct": 0.5,
                "desconto_norm": 1.0,
                "liq_delta_pct": 0.1,
                "liq_delta_norm": 0.5,
            }
        ]
    )

    result = calcular_scores(df_feat)
    assert result.iloc[0]["elegivel_valorizacao"]  # valorização OK
    assert not result.iloc[0]["elegivel_joia"]  # joia NOK (trend < 0)
    assert result.iloc[0]["score_joia_escondida"] == 0.0


# ===========================================================================
# gerar_insights (integração com dados sintéticos)
# ===========================================================================


def test_gerar_insights_cria_json(tmp_path: Path) -> None:
    """Pipeline completo deve gerar JSON com metadata e insights."""
    csv_path = _make_geo_csv(tmp_path)
    out_json = tmp_path / "insights.json"

    gerar_insights(consolidado_geo_csv=csv_path, output_json=out_json)

    assert out_json.exists()
    data = json.loads(out_json.read_text(encoding="utf-8"))
    assert "metadata" in data
    assert "insights" in data
    assert data["metadata"]["versao_formula"] == "v0.1"
    assert isinstance(data["insights"], list)
    assert len(data["insights"]) > 0


def test_gerar_insights_metadata_completa(tmp_path: Path) -> None:
    """Metadata deve conter campos obrigatórios."""
    csv_path = _make_geo_csv(tmp_path)
    out_json = tmp_path / "insights.json"

    gerar_insights(consolidado_geo_csv=csv_path, output_json=out_json)

    data = json.loads(out_json.read_text(encoding="utf-8"))
    meta = data["metadata"]
    assert "versao_formula" in meta
    assert "janelas_meses" in meta
    assert "niveis" in meta
    assert "gerado_em" in meta
    assert "total_insights" in meta


def test_gerar_insights_csv_inexistente_levanta_erro(tmp_path: Path) -> None:
    """CSV inexistente deve levantar FileNotFoundError."""
    with pytest.raises(FileNotFoundError):
        gerar_insights(
            consolidado_geo_csv=tmp_path / "nao_existe.csv",
            output_json=tmp_path / "out.json",
        )


def test_gerar_insights_contem_campos_de_score(tmp_path: Path) -> None:
    """Cada insight deve conter os campos de score esperados."""
    csv_path = _make_geo_csv(tmp_path)
    out_json = tmp_path / "insights.json"

    gerar_insights(consolidado_geo_csv=csv_path, output_json=out_json)

    data = json.loads(out_json.read_text(encoding="utf-8"))
    campos_obrigatorios = [
        "regiao",
        "bairro",
        "score_valorizacao",
        "score_joia_escondida",
        "confianca",
        "selo",
        "nivel",
        "janela_meses",
        "elegivel_valorizacao",
        "elegivel_joia",
    ]
    for insight in data["insights"][:5]:  # amostra dos primeiros
        for campo in campos_obrigatorios:
            assert campo in insight, f"Campo '{campo}' ausente em {insight}"


def test_gerar_insights_niveis_e_janelas(tmp_path: Path) -> None:
    """Insights devem cobrir ambos os níveis e todas as janelas."""
    csv_path = _make_geo_csv(tmp_path)
    out_json = tmp_path / "insights.json"

    gerar_insights(consolidado_geo_csv=csv_path, output_json=out_json)

    data = json.loads(out_json.read_text(encoding="utf-8"))
    niveis = {r["nivel"] for r in data["insights"]}
    janelas = {r["janela_meses"] for r in data["insights"]}

    assert "bairro" in niveis
    assert "logradouro" in niveis
    assert janelas.issubset({12, 24, 36})
    assert len(janelas) >= 2  # At least 2 windows with 5 years of data


# ===========================================================================
# agregar_por_periodo
# ===========================================================================


def test_agregar_por_periodo_bairro() -> None:
    """Agregação por bairro deve somar quantidades por ano."""
    df = pd.DataFrame(
        {
            "BAIRRO": ["Icarai", "Icarai", "Icarai", "Centro"],
            "NOME DO LOGRADOURO": ["Rua A", "Rua B", "Rua A", "Rua C"],
            "ANO DO PAGAMENTO DO ITBI": [2023, 2023, 2024, 2024],
            "VALOR DA TRANSAÇÃO (R$)": [100, 200, 150, 300],
            "QUANTIDADE DE TRANSAÇÕES": [10, 20, 15, 25],
            "VALOR_REAL": [100, 200, 150, 300],
            "NIVEL_GEO": ["endereco", "endereco", "endereco", "bairro"],
        }
    )
    result = agregar_por_periodo(
        df,
        nivel="bairro",
        col_valor="VALOR DA TRANSAÇÃO (R$)",
        col_qtd="QUANTIDADE DE TRANSAÇÕES",
        col_ano="ANO DO PAGAMENTO DO ITBI",
    )
    assert len(result) >= 2  # Icarai (2 anos) + Centro (1 ano)
    icarai_2023 = result[(result["regiao"] == "Icarai") & (result["ano"] == 2023)]
    assert len(icarai_2023) == 1
    assert icarai_2023.iloc[0]["qtd"] == 30  # 10 + 20


# ===========================================================================
# Backtest smoke test
# ===========================================================================


def test_backtest_executa_com_dados_sinteticos(tmp_path: Path) -> None:
    """Backtest deve executar sem erro e gerar report + best config."""
    from itbi.backtest import executar_backtest

    csv_path = _make_geo_csv(tmp_path, anos=[2020, 2021, 2022, 2023, 2024])
    report = tmp_path / "report.json"
    best = tmp_path / "best.json"

    rpt, bst = executar_backtest(
        consolidado_geo_csv=csv_path,
        report_json=report,
        best_json=best,
    )

    assert rpt.exists()
    assert bst.exists()

    report_data = json.loads(rpt.read_text(encoding="utf-8"))
    assert "metadata" in report_data
    assert "resultados" in report_data
    assert len(report_data["resultados"]) > 0

    best_data = json.loads(bst.read_text(encoding="utf-8"))
    assert "pesos_valorizacao" in best_data
    assert "pesos_joia" in best_data
    assert "thresholds" in best_data
    assert "metricas" in best_data


def test_backtest_csv_inexistente_levanta_erro(tmp_path: Path) -> None:
    """CSV inexistente deve levantar FileNotFoundError."""
    from itbi.backtest import executar_backtest

    with pytest.raises(FileNotFoundError):
        executar_backtest(
            consolidado_geo_csv=tmp_path / "nao_existe.csv",
            report_json=tmp_path / "report.json",
            best_json=tmp_path / "best.json",
        )


def test_backtest_poucos_anos_levanta_erro(tmp_path: Path) -> None:
    """Backtest com menos de 3 anos deve levantar ValueError."""
    from itbi.backtest import executar_backtest

    csv_path = _make_geo_csv(tmp_path, anos=[2023, 2024])
    with pytest.raises(ValueError, match="3 anos"):
        executar_backtest(
            consolidado_geo_csv=csv_path,
            report_json=tmp_path / "report.json",
            best_json=tmp_path / "best.json",
        )


# ===========================================================================
# Backtest metrics (unit tests)
# ===========================================================================


def test_spearman_rank_perfeito() -> None:
    """Ranking perfeitamente monotônico deve ter spearman ~1.0."""
    from itbi.backtest import _spearman_rank

    x = [1.0, 2.0, 3.0, 4.0, 5.0]
    y = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert _spearman_rank(x, y) == pytest.approx(1.0)


def test_spearman_rank_invertido() -> None:
    """Ranking invertido deve ter spearman ~-1.0."""
    from itbi.backtest import _spearman_rank

    x = [1.0, 2.0, 3.0, 4.0, 5.0]
    y = [5.0, 4.0, 3.0, 2.0, 1.0]
    assert _spearman_rank(x, y) == pytest.approx(-1.0)


def test_spearman_rank_insuficiente() -> None:
    """Menos de 3 pontos deve retornar 0.0."""
    from itbi.backtest import _spearman_rank

    assert _spearman_rank([1.0, 2.0], [2.0, 1.0]) == 0.0
    assert _spearman_rank([], []) == 0.0


def test_precision_at_k_perfeita() -> None:
    """Top-k com todos corretos deve ter precision 1.0."""
    from itbi.backtest import _precision_at_k

    scores = [10.0, 9.0, 8.0, 7.0, 6.0]
    actuals = [0.5, 0.3, 0.2, 0.1, 0.05]
    assert _precision_at_k(scores, actuals, k=3) == pytest.approx(1.0)


def test_precision_at_k_zero() -> None:
    """Top-k sem acertos deve ter precision 0.0."""
    from itbi.backtest import _precision_at_k

    scores = [10.0, 9.0, 8.0]
    actuals = [-0.1, -0.2, -0.3]
    assert _precision_at_k(scores, actuals, k=3) == pytest.approx(0.0)
