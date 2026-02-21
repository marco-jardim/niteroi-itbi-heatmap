"""
Testes de idempotência do pipeline.

Cobre:
- Resultado idêntico na segunda chamada (cache hit): coordenadas não mudam
- Cache não duplica entradas após reexecuções sucessivas
- Três execuções consecutivas resultam em exatamente uma entrada por endereço
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from itbi.geocodificacao import geocodificar


# ===========================================================================
# Helpers
# ===========================================================================


def _loc(lat: float, lon: float) -> MagicMock:
    """Cria um mock de localização com latitude/longitude."""
    loc = MagicMock()
    loc.latitude = lat
    loc.longitude = lon
    return loc


# ===========================================================================
# Idempotência: resultado idêntico na segunda chamada
# ===========================================================================


def test_geocodificar_resultado_identico_na_segunda_execucao(
    tmp_path: Path,
) -> None:
    """
    Duas execuções com os mesmos dados devem produzir coordenadas idênticas.

    A segunda execução usa o cache em disco; mesmo que o geocodificador esteja
    indisponível (retorna None), o resultado deve ser igual ao da primeira.
    """
    cache_path = tmp_path / "geocache.csv"
    df = pd.DataFrame(
        [{"NOME DO LOGRADOURO": "Rua Coronel Moreira César", "BAIRRO": "Icaraí"}]
    )

    # Primeira execução — geocodifica e persiste no cache
    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
    ):
        mock_rl.return_value.return_value = _loc(-22.9043, -43.1199)
        resultado1 = geocodificar(df.copy(), cache_path=cache_path)

    # Segunda execução — geocodificador falha deliberadamente (cache deve suprir)
    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl2,
    ):
        mock_rl2.return_value.return_value = None
        resultado2 = geocodificar(df.copy(), cache_path=cache_path)

    assert len(resultado1) == len(resultado2) == 1
    assert resultado1["LAT"].iloc[0] == pytest.approx(resultado2["LAT"].iloc[0])
    assert resultado1["LON"].iloc[0] == pytest.approx(resultado2["LON"].iloc[0])
    assert resultado1["NIVEL_GEO"].iloc[0] == resultado2["NIVEL_GEO"].iloc[0]


def test_geocodificar_resultado_identico_com_multiplos_enderecos(
    tmp_path: Path,
) -> None:
    """Dois endereços: segunda execução reproduz exatamente as coordenadas da primeira."""
    cache_path = tmp_path / "geocache.csv"
    df = pd.DataFrame(
        [
            {"NOME DO LOGRADOURO": "Rua Alfa", "BAIRRO": "Centro"},
            {"NOME DO LOGRADOURO": "Rua Beta", "BAIRRO": "Icaraí"},
        ]
    )

    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
    ):
        mock_rl.return_value.side_effect = [
            _loc(-22.897, -43.115),  # Rua Alfa
            _loc(-22.904, -43.120),  # Rua Beta
        ]
        resultado1 = geocodificar(df.copy(), cache_path=cache_path)

    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl2,
    ):
        mock_rl2.return_value.return_value = None  # geocodificador indisponível
        resultado2 = geocodificar(df.copy(), cache_path=cache_path)

    assert len(resultado1) == len(resultado2) == 2

    for col in ("LAT", "LON"):
        for i in range(2):
            assert resultado1[col].iloc[i] == pytest.approx(resultado2[col].iloc[i])


# ===========================================================================
# Idempotência: geocache sem duplicatas
# ===========================================================================


def test_geocache_nao_duplica_entradas_na_segunda_execucao(tmp_path: Path) -> None:
    """
    Reexecutar geocodificar com os mesmos endereços não cria duplicatas no cache.

    O cache é append-only, mas endereços já presentes não devem ser re-inseridos.
    """
    cache_path = tmp_path / "geocache.csv"
    df = pd.DataFrame(
        [
            {"NOME DO LOGRADOURO": "Rua Alfa", "BAIRRO": "Centro"},
            {"NOME DO LOGRADOURO": "Rua Beta", "BAIRRO": "Icaraí"},
        ]
    )

    # Primeira execução — cria as duas entradas
    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
    ):
        mock_rl.return_value.side_effect = [
            _loc(-22.897, -43.115),
            _loc(-22.904, -43.120),
        ]
        geocodificar(df.copy(), cache_path=cache_path)

    df_cache_1 = pd.read_csv(cache_path)
    assert len(df_cache_1) == 2

    # Segunda execução — nenhum endereço novo; cache não deve crescer
    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl2,
    ):
        mock_rl2.return_value.return_value = None
        geocodificar(df.copy(), cache_path=cache_path)

    df_cache_2 = pd.read_csv(cache_path)
    assert len(df_cache_2) == 2, "Cache não deve crescer na segunda execução"
    assert df_cache_2["ENDERECO"].nunique() == 2, "Não deve haver ENDERECOs duplicados"


def test_geocache_sem_duplicata_apos_tres_execucoes(tmp_path: Path) -> None:
    """Três execuções consecutivas não triplicam as entradas do cache."""
    cache_path = tmp_path / "geocache.csv"
    df = pd.DataFrame([{"NOME DO LOGRADOURO": "Rua X", "BAIRRO": "Icaraí"}])
    loc = _loc(-22.9, -43.1)

    for _ in range(3):
        with (
            patch("itbi.geocodificacao.Nominatim"),
            patch("itbi.geocodificacao.RateLimiter") as mock_rl,
        ):
            mock_rl.return_value.return_value = loc
            geocodificar(df.copy(), cache_path=cache_path)

    df_cache = pd.read_csv(cache_path)
    assert len(df_cache) == 1, "Cache deve conter exatamente uma entrada após três runs"
    assert df_cache["ENDERECO"].nunique() == 1
