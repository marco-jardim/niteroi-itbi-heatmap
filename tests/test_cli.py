"""
Testes para itbi.cli.

Cobre:
- cmd_status sem artefatos: todas as linhas exibem 'ausente'
- cmd_status com artefato existente: exibe 'ok' e nome do arquivo
- cmd_run completo: ordem das etapas verificada via side_effect de rastreamento
- cmd_run --skip-download com CSVs presentes: funciona sem download
- cmd_run --skip-download sem CSVs: retorna código de erro 1
"""

import argparse
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from itbi.cli import cmd_run, cmd_status


# ===========================================================================
# cmd_status — sem artefatos
# ===========================================================================


def test_status_sem_artefatos_exibe_ausente(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """Quando nenhum artefato existe, cada linha deve conter 'ausente'."""
    args = argparse.Namespace()

    with (
        patch("itbi.cli.DATA_DIR", tmp_path),
        patch("itbi.cli.GEOCACHE_CSV", tmp_path / "geocache.csv"),
        patch("itbi.cli.OUTPUT_HTML", tmp_path / "index.html"),
        patch("itbi.cli.DATA_JSON", tmp_path / "data" / "itbi_geo.json"),
    ):
        rc = cmd_status(args)

    assert rc == 0
    saida = capsys.readouterr().out
    # Sem nenhum arquivo: CSVs anuais + 4 artefatos individuais = 5 linhas "ausente"
    assert saida.count("ausente") >= 4


# ===========================================================================
# cmd_status — com artefato existente
# ===========================================================================


def test_status_com_consolidado_exibe_ok(
    tmp_path: Path, capsys: pytest.CaptureFixture
) -> None:
    """Artefato existente deve aparecer como 'ok' com o nome correto."""
    consolidado = tmp_path / "consolidado.csv"
    pd.DataFrame([{"BAIRRO": "Icaraí", "NOME DO LOGRADOURO": "Rua X"}]).to_csv(
        consolidado, index=False, encoding="utf-8-sig"
    )

    args = argparse.Namespace()

    with (
        patch("itbi.cli.DATA_DIR", tmp_path),
        patch("itbi.cli.GEOCACHE_CSV", tmp_path / "geocache.csv"),
        patch("itbi.cli.OUTPUT_HTML", tmp_path / "index.html"),
        patch("itbi.cli.DATA_JSON", tmp_path / "data" / "itbi_geo.json"),
    ):
        rc = cmd_status(args)

    assert rc == 0
    saida = capsys.readouterr().out
    assert "ok" in saida
    assert "consolidado.csv" in saida


# ===========================================================================
# cmd_run — ordem das etapas do pipeline
# ===========================================================================


def test_cmd_run_ordem_das_etapas(tmp_path: Path) -> None:
    """Pipeline completo deve chamar todas as etapas na sequência correta."""
    args = argparse.Namespace(
        skip_download=False,
        anos=None,
        skip_geo=False,
        no_markers=False,
    )

    mock_df = pd.DataFrame(
        {
            "NOME DO LOGRADOURO": ["Rua X"],
            "BAIRRO": ["Icaraí"],
            "LAT": [-22.9],
            "LON": [-43.1],
            "NIVEL_GEO": ["endereco"],
        }
    )

    chamadas: list[str] = []

    def fake_descobrir(*_a, **_kw):
        chamadas.append("descobrir")
        return {2024: "http://example.com/2024.csv"}

    def fake_baixar(*_a, **_kw):
        chamadas.append("baixar")
        return [tmp_path / "transacoes_imobiliarias_2024.csv"]

    def fake_consolidar(*_a, **_kw):
        chamadas.append("consolidar")
        return mock_df

    def fake_salvar(*_a, **_kw):
        chamadas.append("salvar")

    def fake_geocodificar(*_a, **_kw):
        chamadas.append("geocodificar")
        return mock_df

    def fake_heatmap(*_a, **_kw):
        chamadas.append("heatmap")

    with (
        patch("itbi.descoberta.descobrir_csv_urls", side_effect=fake_descobrir),
        patch("itbi.download.baixar_csvs", side_effect=fake_baixar),
        patch("itbi.consolidacao.carregar_e_consolidar", side_effect=fake_consolidar),
        patch("itbi.consolidacao.salvar_consolidado", side_effect=fake_salvar),
        patch("itbi.geocodificacao.geocodificar", side_effect=fake_geocodificar),
        patch("itbi.heatmap.gerar_heatmap", side_effect=fake_heatmap),
        patch("itbi.cli.DATA_DIR", tmp_path),
    ):
        rc = cmd_run(args)

    assert rc == 0
    assert chamadas == [
        "descobrir",
        "baixar",
        "consolidar",
        "salvar",
        "geocodificar",
        "heatmap",
    ]


# ===========================================================================
# cmd_run — --skip-download
# ===========================================================================


def test_cmd_run_skip_download_com_csvs_presentes(tmp_path: Path) -> None:
    """--skip-download com CSVs existentes completa o pipeline sem download."""
    args = argparse.Namespace(
        skip_download=True,
        anos=None,
        skip_geo=False,
        no_markers=False,
    )

    # Cria CSV anual fictício para que o glob o encontre
    (tmp_path / "transacoes_imobiliarias_2024.csv").write_text(
        "col\nval\n", encoding="utf-8"
    )

    mock_df = pd.DataFrame(
        {
            "NOME DO LOGRADOURO": ["Rua X"],
            "BAIRRO": ["Icaraí"],
            "LAT": [-22.9],
            "LON": [-43.1],
            "NIVEL_GEO": ["endereco"],
        }
    )

    with (
        patch("itbi.consolidacao.carregar_e_consolidar", return_value=mock_df),
        patch("itbi.consolidacao.salvar_consolidado"),
        patch("itbi.geocodificacao.geocodificar", return_value=mock_df),
        patch("itbi.heatmap.gerar_heatmap"),
        patch("itbi.cli.DATA_DIR", tmp_path),
    ):
        rc = cmd_run(args)

    assert rc == 0


def test_cmd_run_skip_download_sem_csvs_retorna_erro(tmp_path: Path) -> None:
    """--skip-download com diretório vazio deve retornar código 1."""
    args = argparse.Namespace(
        skip_download=True,
        anos=None,
        skip_geo=False,
        no_markers=False,
    )

    with patch("itbi.cli.DATA_DIR", tmp_path):
        rc = cmd_run(args)

    assert rc == 1
