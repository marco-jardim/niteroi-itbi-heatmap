"""
Testes para itbi.geocodificacao.

Cobre:
- _montar_endereco: construção correta da string de endereço
- _montar_endereco_bairro: string de fallback nível 2
- _centroide_bairro: lookup exato e case-insensitive, bairro ausente
- geocodificar: cache hit sem chamar Nominatim
- geocodificar: retrocompatibilidade com cache legado sem NIVEL_GEO
- geocodificar: fallback nível 2 (bairro)
- geocodificar: fallback nível 3 (centroide fixo)
- geocodificar: endereço sem centroide e sem geocodificação → linha descartada
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from itbi.geocodificacao import (
    CENTROIDES_BAIRROS,
    _centroide_bairro,
    _montar_endereco,
    _montar_endereco_bairro,
    geocodificar,
)


# ===========================================================================
# _montar_endereco
# ===========================================================================


class TestMontarEndereco:
    def test_formato_padrao(self) -> None:
        """Formato padrão: logradouro, bairro, Niterói, RJ, Brasil."""
        row = pd.Series(
            {
                "NOME DO LOGRADOURO": "Rua Coronel Moreira César",
                "BAIRRO": "Icaraí",
            }
        )
        resultado = _montar_endereco(row)
        assert resultado == "Rua Coronel Moreira César, Icaraí, Niterói, RJ, Brasil"

    def test_strip_espacos_em_branco(self) -> None:
        """Espaços em branco ao redor de logradouro e bairro são removidos."""
        row = pd.Series(
            {
                "NOME DO LOGRADOURO": "  Av. Amaral Peixoto  ",
                "BAIRRO": "  Centro  ",
            }
        )
        resultado = _montar_endereco(row)
        assert resultado == "Av. Amaral Peixoto, Centro, Niterói, RJ, Brasil"

    def test_bairro_ausente_usa_string_vazia(self) -> None:
        """Bairro ausente resulta em string vazia no campo, sem crash."""
        row = pd.Series({"NOME DO LOGRADOURO": "Rua X"})
        resultado = _montar_endereco(row)
        assert "Rua X" in resultado
        assert "Niterói, RJ, Brasil" in resultado

    def test_logradouro_ausente_usa_string_vazia(self) -> None:
        """Logradouro ausente resulta em string vazia no campo, sem crash."""
        row = pd.Series({"BAIRRO": "Icaraí"})
        resultado = _montar_endereco(row)
        assert "Icaraí" in resultado
        assert "Niterói, RJ, Brasil" in resultado

    def test_sufixo_fixo_cidade_uf_pais(self) -> None:
        """Sufixo ',  Niterói, RJ, Brasil' está sempre presente."""
        row = pd.Series({"NOME DO LOGRADOURO": "A", "BAIRRO": "B"})
        assert _montar_endereco(row).endswith("Niterói, RJ, Brasil")


# ===========================================================================
# _montar_endereco_bairro
# ===========================================================================


def test_montar_endereco_bairro_formato_correto() -> None:
    """String de bairro segue o formato '<bairro>, Niterói, RJ, Brasil'."""
    assert _montar_endereco_bairro("Icaraí") == "Icaraí, Niterói, RJ, Brasil"
    assert _montar_endereco_bairro("Centro") == "Centro, Niterói, RJ, Brasil"


# ===========================================================================
# _centroide_bairro
# ===========================================================================


class TestCentroideBairro:
    def test_lookup_exato(self) -> None:
        """Bairro com grafia exata retorna coordenadas corretas."""
        resultado = _centroide_bairro("Icaraí")
        assert resultado is not None
        lat, lon = resultado
        assert lat == pytest.approx(CENTROIDES_BAIRROS["Icaraí"][0])
        assert lon == pytest.approx(CENTROIDES_BAIRROS["Icaraí"][1])

    def test_lookup_case_insensitive(self) -> None:
        """Lookup funciona independentemente de capitalização."""
        resultado = _centroide_bairro("ICARAÍ")
        assert resultado is not None
        lat, lon = resultado
        assert lat == pytest.approx(CENTROIDES_BAIRROS["Icaraí"][0])

    def test_bairro_inexistente_retorna_none(self) -> None:
        """Bairro não mapeado retorna None, sem exceção."""
        assert _centroide_bairro("BairroFicticioInexistente999") is None

    def test_bairro_centro_mapeado(self) -> None:
        """Centro (bairro mais comum) deve estar no dicionário."""
        resultado = _centroide_bairro("Centro")
        assert resultado is not None


# ===========================================================================
# geocodificar — cache hit
# ===========================================================================


def test_geocodificar_usa_cache_sem_chamar_nominatim(tmp_path: Path) -> None:
    """Endereço já no cache não deve acionar o Nominatim."""
    cache_path = tmp_path / "geocache.csv"
    pd.DataFrame(
        [
            {
                "ENDERECO": "Rua X, Icaraí, Niterói, RJ, Brasil",
                "LAT": -22.9000,
                "LON": -43.1000,
                "NIVEL_GEO": "endereco",
            }
        ]
    ).to_csv(cache_path, index=False, encoding="utf-8-sig")

    df = pd.DataFrame([{"NOME DO LOGRADOURO": "Rua X", "BAIRRO": "Icaraí"}])

    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
    ):
        mock_geocode = mock_rl.return_value
        resultado = geocodificar(df, cache_path=cache_path)

    # Nominatim não deve ser consultado para endereço em cache
    mock_geocode.assert_not_called()
    assert len(resultado) == 1
    assert resultado["LAT"].iloc[0] == pytest.approx(-22.9000)
    assert resultado["LON"].iloc[0] == pytest.approx(-43.1000)
    assert resultado["NIVEL_GEO"].iloc[0] == "endereco"


def test_geocodificar_cache_retrocompatibilidade_sem_nivel_geo(
    tmp_path: Path,
) -> None:
    """Cache legado (sem coluna NIVEL_GEO) deve ser lido sem erro."""
    cache_path = tmp_path / "geocache.csv"
    # Escreve cache sem coluna NIVEL_GEO — usa pandas para quoting correto de vírgulas
    pd.DataFrame(
        [{"ENDERECO": "Rua X, Icaraí, Niterói, RJ, Brasil", "LAT": -22.9, "LON": -43.1}]
    ).to_csv(cache_path, index=False, encoding="utf-8")

    df = pd.DataFrame([{"NOME DO LOGRADOURO": "Rua X", "BAIRRO": "Icaraí"}])

    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
    ):
        mock_rl.return_value.return_value = None
        resultado = geocodificar(df, cache_path=cache_path)

    # Entrada legada deve ser usada; nenhuma chamada ao geocodificador
    assert len(resultado) == 1
    assert resultado["LAT"].iloc[0] == pytest.approx(-22.9)


# ===========================================================================
# geocodificar — fallback nível 2 (bairro)
# ===========================================================================


def test_geocodificar_fallback_nivel2_bairro(tmp_path: Path) -> None:
    """Quando nível 1 falha (None), geocodifica por bairro e registra NIVEL_GEO='bairro'."""
    cache_path = tmp_path / "geocache.csv"
    df = pd.DataFrame([{"NOME DO LOGRADOURO": "Rua Inexistente", "BAIRRO": "Icaraí"}])

    loc_bairro = MagicMock()
    loc_bairro.latitude = -22.9043
    loc_bairro.longitude = -43.1199

    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
    ):
        # nível 1 → None; nível 2 → loc_bairro
        mock_rl.return_value.side_effect = [None, loc_bairro]
        resultado = geocodificar(df, cache_path=cache_path)

    assert len(resultado) == 1
    assert resultado["NIVEL_GEO"].iloc[0] == "bairro"
    assert resultado["LAT"].iloc[0] == pytest.approx(-22.9043)
    assert resultado["LON"].iloc[0] == pytest.approx(-43.1199)


# ===========================================================================
# geocodificar — fallback nível 3 (centroide fixo)
# ===========================================================================


def test_geocodificar_fallback_nivel3_centroide(tmp_path: Path) -> None:
    """Quando níveis 1 e 2 falham, usa o centroide fixo do bairro."""
    cache_path = tmp_path / "geocache.csv"
    df = pd.DataFrame([{"NOME DO LOGRADOURO": "Rua Qualquer", "BAIRRO": "Icaraí"}])

    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
    ):
        # Todos os níveis via Nominatim falham
        mock_rl.return_value.return_value = None
        resultado = geocodificar(df, cache_path=cache_path)

    lat_esperada, lon_esperada = CENTROIDES_BAIRROS["Icaraí"]
    assert len(resultado) == 1
    assert resultado["NIVEL_GEO"].iloc[0] == "centroide"
    assert resultado["LAT"].iloc[0] == pytest.approx(lat_esperada)
    assert resultado["LON"].iloc[0] == pytest.approx(lon_esperada)


def test_geocodificar_sem_centroide_descarta_linha(tmp_path: Path) -> None:
    """Endereço de bairro sem centroide e sem geocodificação não aparece no resultado."""
    cache_path = tmp_path / "geocache.csv"
    df = pd.DataFrame(
        [{"NOME DO LOGRADOURO": "Rua X", "BAIRRO": "BairroFicticioXXX999"}]
    )

    with (
        patch("itbi.geocodificacao.Nominatim"),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
    ):
        mock_rl.return_value.return_value = None
        resultado = geocodificar(df, cache_path=cache_path)

    assert len(resultado) == 0


# ===========================================================================
# geocodificar — backend geocodebr
# ===========================================================================


def test_geocodificar_com_geocodebr_em_lote(tmp_path: Path) -> None:
    """Quando geocodebr está disponível, usa lote sem chamar Nominatim."""
    cache_path = tmp_path / "geocache.csv"
    df = pd.DataFrame([{"NOME DO LOGRADOURO": "Rua X", "BAIRRO": "Icaraí"}])
    endereco = "Rua X, Icaraí, Niterói, RJ, Brasil"

    with (
        patch("itbi.geocodificacao._geocodebr_disponivel", return_value=True),
        patch(
            "itbi.geocodificacao._geocodificar_lote_geocodebr",
            return_value={endereco: (-22.9, -43.1, "endereco")},
        ) as mock_lote,
        patch("itbi.geocodificacao.Nominatim") as mock_nom,
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
    ):
        resultado = geocodificar(df, cache_path=cache_path, geocoder="geocodebr")

    mock_lote.assert_called_once()
    mock_nom.assert_not_called()
    mock_rl.assert_not_called()
    assert len(resultado) == 1
    assert resultado["NIVEL_GEO"].iloc[0] == "endereco"


def test_geocodificar_geocodebr_indisponivel_retorna_nominatim(tmp_path: Path) -> None:
    """geocodebr indisponível recua para Nominatim sem quebrar pipeline."""
    cache_path = tmp_path / "geocache.csv"
    df = pd.DataFrame([{"NOME DO LOGRADOURO": "Rua Y", "BAIRRO": "Centro"}])

    loc = MagicMock()
    loc.latitude = -22.9
    loc.longitude = -43.1

    with (
        patch("itbi.geocodificacao._geocodebr_disponivel", return_value=False),
        patch("itbi.geocodificacao.RateLimiter") as mock_rl,
        patch("itbi.geocodificacao.Nominatim"),
    ):
        mock_rl.return_value.return_value = loc
        resultado = geocodificar(df, cache_path=cache_path, geocoder="geocodebr")

    assert len(resultado) == 1
    assert resultado["NIVEL_GEO"].iloc[0] == "endereco"


def test_geocodificar_geocoder_invalido_levanta_erro(tmp_path: Path) -> None:
    """Backend inválido deve gerar ValueError claro."""
    cache_path = tmp_path / "geocache.csv"
    df = pd.DataFrame([{"NOME DO LOGRADOURO": "Rua Z", "BAIRRO": "Icaraí"}])

    with pytest.raises(ValueError):
        geocodificar(df, cache_path=cache_path, geocoder="invalido")
