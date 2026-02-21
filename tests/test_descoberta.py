"""
Testes para itbi.descoberta.

Cobre:
- fallback quando requests lança ConnectionError
- fallback quando requests lança Timeout
- fallback quando raise_for_status lança HTTPError
- parse de HTML com div.entry-content → extrai link e ano corretamente
- parse de HTML sem container conhecido → cai na página inteira
- HTML sem links CSV → retorna CSV_URLS_FALLBACK
- múltiplos anos extraídos corretamente
- link relativo no href → resolvido para URL absoluta via urljoin
- chaves do dicionário retornado são int (anos)
"""

from unittest.mock import MagicMock, patch

import requests

from itbi.config import CSV_URLS_FALLBACK
from itbi.descoberta import descobrir_csv_urls

# ===========================================================================
# Helpers
# ===========================================================================

_BASE = "https://exemplo.com/itbi/"


def _resp(html: str) -> MagicMock:
    """Cria mock de Response HTTP com status 200 e HTML fornecido."""
    mock = MagicMock()
    mock.text = html
    mock.raise_for_status.return_value = None
    return mock


def _html_entry_content(*anos: int) -> str:
    """HTML com div.entry-content contendo links para os anos fornecidos."""
    links = "".join(
        f'<a href="files/transacoes_imobiliarias_{ano}.csv">CSV {ano}</a>'
        for ano in anos
    )
    return f"<html><body><div class='entry-content'>{links}</div></body></html>"


# ===========================================================================
# Fallback: erros de rede e HTTP
# ===========================================================================


def test_fallback_connection_error() -> None:
    """ConnectionError ao acessar a página → retorna CSV_URLS_FALLBACK."""
    with patch(
        "itbi.descoberta.requests.get",
        side_effect=requests.ConnectionError("recusada"),
    ):
        resultado = descobrir_csv_urls()
    assert resultado == CSV_URLS_FALLBACK


def test_fallback_timeout() -> None:
    """Timeout na requisição → retorna CSV_URLS_FALLBACK."""
    with patch(
        "itbi.descoberta.requests.get",
        side_effect=requests.Timeout("timeout"),
    ):
        resultado = descobrir_csv_urls()
    assert resultado == CSV_URLS_FALLBACK


def test_fallback_http_error_via_raise_for_status() -> None:
    """HTTPError levantado por raise_for_status → retorna CSV_URLS_FALLBACK."""
    mock = MagicMock()
    mock.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
    with patch("itbi.descoberta.requests.get", return_value=mock):
        resultado = descobrir_csv_urls()
    assert resultado == CSV_URLS_FALLBACK


# ===========================================================================
# Parse HTML — extração de links
# ===========================================================================


def test_parse_html_extrai_ano_correto() -> None:
    """HTML com div.entry-content contendo um link → ano correto extraído."""
    html = _html_entry_content(2023)
    with patch("itbi.descoberta.requests.get", return_value=_resp(html)):
        resultado = descobrir_csv_urls(url=_BASE)

    assert 2023 in resultado
    assert "transacoes_imobiliarias_2023.csv" in resultado[2023]


def test_parse_html_url_e_absoluta() -> None:
    """URL extraída do link relativo deve ser absoluta (começa com http)."""
    html = _html_entry_content(2024)
    with patch("itbi.descoberta.requests.get", return_value=_resp(html)):
        resultado = descobrir_csv_urls(url=_BASE)

    assert resultado[2024].startswith("http")


def test_parse_html_multiplos_anos_todos_extraidos() -> None:
    """Múltiplos links no HTML → todos os anos extraídos sem perda."""
    html = _html_entry_content(2020, 2021, 2022, 2023, 2024)
    with patch("itbi.descoberta.requests.get", return_value=_resp(html)):
        resultado = descobrir_csv_urls(url=_BASE)

    assert set(resultado.keys()) == {2020, 2021, 2022, 2023, 2024}


def test_parse_html_sem_container_usa_pagina_inteira() -> None:
    """HTML sem div.entry-content → cai na página inteira, ainda extrai links."""
    html = (
        "<html><body>"
        "<a href='files/transacoes_imobiliarias_2021.csv'>CSV</a>"
        "</body></html>"
    )
    with patch("itbi.descoberta.requests.get", return_value=_resp(html)):
        resultado = descobrir_csv_urls(url=_BASE)

    assert 2021 in resultado


def test_parse_html_com_post_content_div() -> None:
    """div.post-content é o segundo seletor de fallback; deve extrair links."""
    html = (
        "<html><body><div class='post-content'>"
        "<a href='files/transacoes_imobiliarias_2020.csv'>CSV</a>"
        "</div></body></html>"
    )
    with patch("itbi.descoberta.requests.get", return_value=_resp(html)):
        resultado = descobrir_csv_urls(url=_BASE)

    assert 2020 in resultado


def test_parse_html_sem_links_csv_retorna_fallback() -> None:
    """HTML sem nenhum link CSV → retorna CSV_URLS_FALLBACK."""
    html = (
        "<html><body><div class='entry-content'>"
        "<a href='/sobre'>Sobre</a>"
        "<a href='/contato'>Contato</a>"
        "</div></body></html>"
    )
    with patch("itbi.descoberta.requests.get", return_value=_resp(html)):
        resultado = descobrir_csv_urls()

    assert resultado == CSV_URLS_FALLBACK


def test_parse_html_link_relativo_com_barra_absoluta() -> None:
    """Link com caminho absoluto (/wp-content/...) → URL absoluta via urljoin."""
    html = (
        "<html><body><div class='entry-content'>"
        "<a href='/wp-content/uploads/transacoes_imobiliarias_2022.csv'>CSV</a>"
        "</div></body></html>"
    )
    with patch("itbi.descoberta.requests.get", return_value=_resp(html)):
        resultado = descobrir_csv_urls(url=_BASE)

    assert 2022 in resultado
    # urljoin com caminho absoluto usa apenas o host da URL base
    assert "transacoes_imobiliarias_2022" in resultado[2022]
    assert resultado[2022].startswith("https://exemplo.com")


def test_chaves_do_resultado_sao_inteiros() -> None:
    """O dicionário retornado deve ter chaves int (anos), nunca strings."""
    html = _html_entry_content(2024)
    with patch("itbi.descoberta.requests.get", return_value=_resp(html)):
        resultado = descobrir_csv_urls(url=_BASE)

    for chave in resultado:
        assert isinstance(chave, int), (
            f"Esperava int, mas chave é {type(chave).__name__!r}: {chave!r}"
        )


def test_fallback_contem_todos_os_anos_esperados() -> None:
    """CSV_URLS_FALLBACK deve ter chaves inteiras e URLs com o padrão esperado."""
    for ano, url in CSV_URLS_FALLBACK.items():
        assert isinstance(ano, int)
        assert f"transacoes_imobiliarias_{ano}" in url
        assert url.startswith("http")
