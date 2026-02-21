"""
Testes de recuperação do pipeline.

Cobre:
- reset_cache=True: cria backup automático do cache existente antes de apagar
- reset_cache=True sem cache existente: não cria arquivo de backup
- Cache com ParserError (corrupção de arquivo): inicia com cache vazio e geocodifica
- Cache com entradas parcialmente inválidas (lat/lon não numérico): ignora silenciosamente
- Geocodificação parcial: segunda execução retoma os endereços não geocodificados
- GeocoderTimedOut: usa centroide do bairro como fallback de recuperação
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from geopy.exc import GeocoderTimedOut

from itbi.geocodificacao import CENTROIDES_BAIRROS, geocodificar


# ===========================================================================
# Helpers
# ===========================================================================


def _loc(lat: float, lon: float) -> MagicMock:
    loc = MagicMock()
    loc.latitude = lat
    loc.longitude = lon
    return loc


# ===========================================================================
# reset_cache: backup automático
# ===========================================================================


def test_reset_cache_cria_backup_do_cache_existente(tmp_path: Path) -> None:
    """reset_cache=True deve criar '<cache>.backup.csv' com o conteúdo original."""
    cache_path = tmp_path / "geocache.csv"
    pd.DataFrame(
        [
            {
                "ENDERECO": "Rua Original, Icaraí, Niterói, RJ, Brasil",
                "LAT": -22.9,
                "LON": -43.1,
                "NIVEL_GEO": "endereco",
            }
        ]
    ).to_csv(cache_path, index=False, encoding="utf-8-sig")

    df = pd.DataFrame([{"NOME DO LOGRADOURO": "Rua Nova", "BAIRRO": "Centro"}])

    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
    ):
        mock_rl.return_value.return_value = None  # geocodificação falha
        geocodificar(df, cache_path=cache_path, reset_cache=True)

    backup = cache_path.with_suffix(".backup.csv")
    assert backup.exists(), "Arquivo de backup deve ser criado junto ao cache original"

    df_backup = pd.read_csv(backup)
    assert (
        "Rua Original, Icaraí, Niterói, RJ, Brasil" in df_backup["ENDERECO"].values
    ), "Backup deve preservar o conteúdo original do cache"


def test_reset_cache_backup_preserva_conteudo_byte_a_byte(tmp_path: Path) -> None:
    """Backup é cópia fiel (shutil.copy2); bytes devem ser idênticos ao original."""
    cache_path = tmp_path / "geocache.csv"
    original_df = pd.DataFrame(
        [
            {
                "ENDERECO": "Rua A, Centro, Niterói, RJ, Brasil",
                "LAT": -22.9,
                "LON": -43.1,
                "NIVEL_GEO": "endereco",
            },
            {
                "ENDERECO": "Rua B, Icaraí, Niterói, RJ, Brasil",
                "LAT": -22.8,
                "LON": -43.0,
                "NIVEL_GEO": "bairro",
            },
        ]
    )
    original_df.to_csv(cache_path, index=False, encoding="utf-8-sig")
    conteudo_original = cache_path.read_bytes()

    df = pd.DataFrame([{"NOME DO LOGRADOURO": "Rua C", "BAIRRO": "Icaraí"}])

    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
    ):
        mock_rl.return_value.return_value = None
        geocodificar(df, cache_path=cache_path, reset_cache=True)

    backup = cache_path.with_suffix(".backup.csv")
    assert backup.read_bytes() == conteudo_original


def test_reset_cache_sem_cache_existente_nao_cria_backup(tmp_path: Path) -> None:
    """reset_cache=True sem cache existente não deve criar arquivo de backup."""
    cache_path = tmp_path / "geocache.csv"
    assert not cache_path.exists()

    df = pd.DataFrame([{"NOME DO LOGRADOURO": "Rua X", "BAIRRO": "Icaraí"}])

    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
    ):
        mock_rl.return_value.return_value = None
        geocodificar(df, cache_path=cache_path, reset_cache=True)

    backup = cache_path.with_suffix(".backup.csv")
    assert not backup.exists(), "Sem cache original, não deve existir arquivo de backup"


# ===========================================================================
# Cache corrompido: fallback para cache vazio
# ===========================================================================


def test_cache_corrompido_parser_error_inicia_vazio(tmp_path: Path) -> None:
    """
    Quando pd.read_csv levanta ParserError (arquivo corrompido),
    a função deve ignorar o cache e geocodificar do zero.
    """
    cache_path = tmp_path / "geocache.csv"
    cache_path.write_text("DUMMY_FILE_EXISTS", encoding="utf-8")

    df = pd.DataFrame([{"NOME DO LOGRADOURO": "Rua X", "BAIRRO": "Icaraí"}])
    loc = _loc(-22.9, -43.1)

    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
        patch.object(
            pd,
            "read_csv",
            side_effect=pd.errors.ParserError("cache corrompido simulado"),
        ),
    ):
        mock_rl.return_value.return_value = loc
        resultado = geocodificar(df, cache_path=cache_path)

    assert len(resultado) == 1
    assert resultado["LAT"].iloc[0] == pytest.approx(-22.9)


def test_cache_com_entradas_lat_lon_invalidos_ignoradas(tmp_path: Path) -> None:
    """
    Entradas com lat/lon não numérico são ignoradas silenciosamente;
    o endereço afetado é re-geocodificado normalmente.
    """
    cache_path = tmp_path / "geocache.csv"
    # Endereços contêm vírgulas — todos os campos devem estar entre aspas duplas
    # para que o parser CSV os leia corretamente.
    cache_path.write_text(
        '"ENDERECO","LAT","LON","NIVEL_GEO"\n'
        '"Rua Valida, Icaraí, Niterói, RJ, Brasil",-22.9,-43.1,"endereco"\n'
        '"Rua Invalida, BairroX, Niterói, RJ, Brasil","bad_lat","bad_lon","endereco"\n',
        encoding="utf-8",
    )

    df = pd.DataFrame(
        [
            {"NOME DO LOGRADOURO": "Rua Valida", "BAIRRO": "Icaraí"},
            {"NOME DO LOGRADOURO": "Rua Invalida", "BAIRRO": "BairroX"},
        ]
    )
    loc_novo = _loc(-22.85, -43.09)

    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
    ):
        # "Rua Valida" vem do cache; "Rua Invalida" será geocodificada do zero
        mock_rl.return_value.return_value = loc_novo
        resultado = geocodificar(df, cache_path=cache_path)

    assert len(resultado) == 2
    rua_valida = resultado[resultado["NOME DO LOGRADOURO"] == "Rua Valida"].iloc[0]
    assert rua_valida["LAT"] == pytest.approx(-22.9)
    rua_invalida = resultado[resultado["NOME DO LOGRADOURO"] == "Rua Invalida"].iloc[0]
    assert rua_invalida["LAT"] == pytest.approx(-22.85)


# ===========================================================================
# Geocodificação parcial: retomada após falha
# ===========================================================================


def test_geocodificacao_retomada_apos_falha_parcial(tmp_path: Path) -> None:
    """
    Endereço não geocodificado na primeira rodada deve ser retomado na segunda,
    sem re-geocodificar o que já está no cache.
    """
    cache_path = tmp_path / "geocache.csv"

    # Bairro sem centroide garante que falha total não é salva no cache
    BAIRRO_SEM_CENTROIDE = "BairroFicticioSemCentroide"
    assert BAIRRO_SEM_CENTROIDE not in CENTROIDES_BAIRROS

    df = pd.DataFrame(
        [
            {"NOME DO LOGRADOURO": "Rua Sucesso", "BAIRRO": "Icaraí"},
            {"NOME DO LOGRADOURO": "Rua Falha", "BAIRRO": BAIRRO_SEM_CENTROIDE},
        ]
    )

    # --- Primeira execução ---
    # Rua Sucesso → geocodificada (nível 1 ok)
    # Rua Falha → None nos dois níveis + sem centroide → não salva
    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
    ):
        mock_rl.return_value.side_effect = [
            _loc(-22.9, -43.1),  # Rua Sucesso — nível 1
            None,  # Rua Falha — nível 1
            None,  # Rua Falha — nível 2
        ]
        resultado1 = geocodificar(df.copy(), cache_path=cache_path)

    assert len(resultado1) == 1
    assert resultado1.iloc[0]["NOME DO LOGRADOURO"] == "Rua Sucesso"

    df_cache_apos1 = pd.read_csv(cache_path)
    assert len(df_cache_apos1) == 1, "Somente 'Rua Sucesso' deve estar no cache"

    # --- Segunda execução ---
    # Rua Sucesso → cache hit (sem chamar geocodificador)
    # Rua Falha → agora geocodificada com sucesso
    loc_retomada = _loc(-22.85, -43.09)

    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl2,
    ):
        mock_rl2.return_value.return_value = loc_retomada
        resultado2 = geocodificar(df.copy(), cache_path=cache_path)

    assert len(resultado2) == 2

    rua_sucesso = resultado2[resultado2["NOME DO LOGRADOURO"] == "Rua Sucesso"].iloc[0]
    assert rua_sucesso["LAT"] == pytest.approx(-22.9), "Cache hit deve preservar coords"

    rua_falha = resultado2[resultado2["NOME DO LOGRADOURO"] == "Rua Falha"].iloc[0]
    assert rua_falha["LAT"] == pytest.approx(-22.85), "Segunda rodada deve geocodificar"

    df_cache_apos2 = pd.read_csv(cache_path)
    assert len(df_cache_apos2) == 2, "Cache deve crescer para 2 após segunda execução"


# ===========================================================================
# GeocoderTimedOut: recuperação via centroide
# ===========================================================================


def test_timeout_geocoder_usa_centroide_como_fallback(tmp_path: Path) -> None:
    """
    Timeout no Nominatim deve acionar o fallback de centroide para não perder o ponto.
    O resultado deve ter NIVEL_GEO='centroide' e coordenadas do bairro.
    """
    cache_path = tmp_path / "geocache.csv"
    df = pd.DataFrame([{"NOME DO LOGRADOURO": "Rua X", "BAIRRO": "Icaraí"}])

    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
    ):
        mock_rl.return_value.side_effect = GeocoderTimedOut("timeout simulado")
        resultado = geocodificar(df, cache_path=cache_path)

    lat_esp, lon_esp = CENTROIDES_BAIRROS["Icaraí"]
    assert len(resultado) == 1
    assert resultado["NIVEL_GEO"].iloc[0] == "centroide"
    assert resultado["LAT"].iloc[0] == pytest.approx(lat_esp)
    assert resultado["LON"].iloc[0] == pytest.approx(lon_esp)


def test_timeout_sem_centroide_descarta_linha(tmp_path: Path) -> None:
    """
    Timeout sem centroide disponível resulta em linha descartada, sem crash.
    """
    cache_path = tmp_path / "geocache.csv"
    df = pd.DataFrame(
        [{"NOME DO LOGRADOURO": "Rua Y", "BAIRRO": "BairroSemCentroideXYZ"}]
    )

    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
    ):
        mock_rl.return_value.side_effect = GeocoderTimedOut("timeout simulado")
        resultado = geocodificar(df, cache_path=cache_path)

    assert len(resultado) == 0, "Linha sem centroide após timeout deve ser descartada"
