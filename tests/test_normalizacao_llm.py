"""Testes unitários para itbi/normalizacao_llm.py."""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from itbi.normalizacao_llm import (
    _normalizar_batch,
    carregar_normalizados,
    normalizar_enderecos_llm,
    ENDERECOS_NORM_JSON,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_ENDERECO_A = "Rua Tiradentes, Centro, Niterói, RJ, Brasil"
_ENDERECO_B = "Av. Ernani Amaral Peixoto, São Domingos, Niterói, RJ, Brasil"

_MOCK_RESPOSTA_API = {
    _ENDERECO_A: {
        "logradouro": "Rua Tiradentes",
        "numero": "",
        "complemento": "",
        "bairro": "Centro",
        "municipio": "Niterói",
        "estado": "RJ",
        "cep": "",
    },
    _ENDERECO_B: {
        "logradouro": "Avenida Ernani Amaral Peixoto",
        "numero": "",
        "complemento": "",
        "bairro": "São Domingos",
        "municipio": "Niterói",
        "estado": "RJ",
        "cep": "",
    },
}


# ---------------------------------------------------------------------------
# _normalizar_batch
# ---------------------------------------------------------------------------


def _mock_response(payload: dict) -> MagicMock:
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    content = json.dumps(payload)
    resp.json.return_value = {"choices": [{"message": {"content": content}}]}
    return resp


def test_normalizar_batch_retorna_campos_esperados():
    """Batch deve retornar dict com campos estruturados por endereço."""
    enderecos = [_ENDERECO_A, _ENDERECO_B]

    with patch("requests.post", return_value=_mock_response(_MOCK_RESPOSTA_API)):
        resultado = _normalizar_batch(enderecos, api_key="fake-key")

    assert set(resultado.keys()) == set(enderecos)
    assert resultado[_ENDERECO_A]["logradouro"] == "Rua Tiradentes"
    assert resultado[_ENDERECO_B]["bairro"] == "São Domingos"


def test_normalizar_batch_fallback_em_erro_de_api():
    """Falha na API deve retornar defaults vazios sem lançar exceção."""
    with patch("requests.post", side_effect=Exception("Timeout")):
        resultado = _normalizar_batch([_ENDERECO_A], api_key="fake-key")

    assert _ENDERECO_A in resultado
    # municipio e estado devem ter valores padrão definidos
    assert resultado[_ENDERECO_A].get("municipio") == "Niterói"
    assert resultado[_ENDERECO_A].get("estado") == "RJ"


def test_normalizar_batch_strip_markdown_code_block():
    """Resposta com bloco ```json deve ser parseada corretamente."""
    payload_str = (
        "```json\n"
        + json.dumps({_ENDERECO_A: _MOCK_RESPOSTA_API[_ENDERECO_A]})
        + "\n```"
    )
    resp = MagicMock()
    resp.raise_for_status = MagicMock()
    resp.json.return_value = {"choices": [{"message": {"content": payload_str}}]}

    with patch("requests.post", return_value=resp):
        resultado = _normalizar_batch([_ENDERECO_A], api_key="fake-key")

    assert resultado[_ENDERECO_A]["logradouro"] == "Rua Tiradentes"


# ---------------------------------------------------------------------------
# carregar_normalizados
# ---------------------------------------------------------------------------


def test_carregar_normalizados_arquivo_ausente():
    """Deve retornar dict vazio quando o arquivo não existe."""
    resultado = carregar_normalizados(Path("/caminho/inexistente/xyz.json"))
    assert resultado == {}


def test_carregar_normalizados_leitura(tmp_path):
    """Deve carregar o JSON e retornar dict corretamente."""
    p = tmp_path / "enderecos_normalizados.json"
    p.write_text(json.dumps(_MOCK_RESPOSTA_API), encoding="utf-8")

    resultado = carregar_normalizados(p)
    assert resultado[_ENDERECO_A]["logradouro"] == "Rua Tiradentes"


# ---------------------------------------------------------------------------
# normalizar_enderecos_llm (integração com cache incremental)
# ---------------------------------------------------------------------------


def test_normalizar_enderecos_llm_usa_cache_existente(tmp_path):
    """Endereços já presentes no cache não devem acionar a API."""
    cache_path = tmp_path / "enderecos_normalizados.json"
    cache_path.write_text(
        json.dumps({_ENDERECO_A: _MOCK_RESPOSTA_API[_ENDERECO_A]}), encoding="utf-8"
    )

    df = pd.DataFrame({"ENDERECO": [_ENDERECO_A]})

    with patch("requests.post") as mock_post:
        resultado = normalizar_enderecos_llm(
            df,
            output_path=cache_path,
            api_key="fake-key",
        )
        mock_post.assert_not_called()  # cache hit total — sem chamada de API

    assert _ENDERECO_A in resultado


def test_normalizar_enderecos_llm_normaliza_novos(tmp_path):
    """Endereços novos devem ser enviados à API e salvos no JSON."""
    cache_path = tmp_path / "enderecos_normalizados.json"
    df = pd.DataFrame({"ENDERECO": [_ENDERECO_A]})

    with patch(
        "requests.post",
        return_value=_mock_response({_ENDERECO_A: _MOCK_RESPOSTA_API[_ENDERECO_A]}),
    ):
        resultado = normalizar_enderecos_llm(
            df,
            output_path=cache_path,
            api_key="fake-key",
            batch_size=10,
        )

    assert _ENDERECO_A in resultado
    assert cache_path.exists()
    salvo = json.loads(cache_path.read_text(encoding="utf-8"))
    assert _ENDERECO_A in salvo


def test_normalizar_enderecos_llm_salva_incrementalmente(tmp_path):
    """Cache deve ser persistido incrementalmente a cada batch."""
    cache_path = tmp_path / "enderecos_normalizados.json"
    enderecos = [_ENDERECO_A, _ENDERECO_B]
    df = pd.DataFrame({"ENDERECO": enderecos})

    batch_call_count = []

    original = _normalizar_batch

    def fake_batch(end_list, api_key, **kwargs):
        batch_call_count.append(len(end_list))
        return {e: _MOCK_RESPOSTA_API[e] for e in end_list if e in _MOCK_RESPOSTA_API}

    with patch("itbi.normalizacao_llm._normalizar_batch", side_effect=fake_batch):
        resultado = normalizar_enderecos_llm(
            df,
            output_path=cache_path,
            api_key="fake-key",
            batch_size=1,  # batch_size=1 força 2 batches
        )

    assert len(batch_call_count) == 2
    assert len(resultado) == 2
