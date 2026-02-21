"""
Testes de contrato da fonte de dados ITBI Niterói.

Cobre:
- Schema mínimo: ValueError acionável com nome das colunas ausentes
- Schema completo: sem exceção quando colunas obrigatórias presentes
- COLUNAS_REQUERIDAS contém as colunas necessárias ao pipeline de geocodificação
- Parse HTML de descoberta: fixture realista extraída corretamente
  (chaves int, URLs absolutas, padrão de nome de arquivo correto)
- Separador e encoding: consolidação robusta a todas as combinações sem crash
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from itbi.consolidacao import COLUNAS_REQUERIDAS, carregar_e_consolidar, validar_schema
from itbi.descoberta import descobrir_csv_urls

# ===========================================================================
# Helpers
# ===========================================================================

_URL_BASE = (
    "https://www.fazenda.niteroi.rj.gov.br/site/dados-das-transacoes-imobiliarias/"
)

# Fixture HTML realista que imita a estrutura WordPress da SMF Niterói
_HTML_FIXTURE = """\
<!DOCTYPE html>
<html lang="pt-BR">
<head><title>Dados das Transações Imobiliárias</title></head>
<body>
<div class="entry-content">
  <h2>Download dos Dados de Transações Imobiliárias</h2>
  <p>Selecione o ano:</p>
  <ul>
    <li><a href="/site/wp-content/uploads/2025/02/transacoes_imobiliarias_2020.csv">2020 (CSV)</a></li>
    <li><a href="/site/wp-content/uploads/2025/02/transacoes_imobiliarias_2021.csv">2021 (CSV)</a></li>
    <li><a href="/site/wp-content/uploads/2025/02/transacoes_imobiliarias_2022.csv">2022 (CSV)</a></li>
    <li><a href="/site/wp-content/uploads/2025/02/transacoes_imobiliarias_2023.csv">2023 (CSV)</a></li>
    <li><a href="/site/wp-content/uploads/2025/02/transacoes_imobiliarias_2024.csv">2024 (CSV)</a></li>
  </ul>
</div>
</body>
</html>
"""


def _mock_resp(html: str) -> MagicMock:
    """Cria mock de Response HTTP com HTML fornecido e sem erro."""
    mock = MagicMock()
    mock.text = html
    mock.raise_for_status.return_value = None
    return mock


# ===========================================================================
# Schema obrigatório — validar_schema
# ===========================================================================


class TestSchemaObrigatorio:
    """Valida que colunas mínimas do pipeline são detectadas e reportadas."""

    def test_coluna_bairro_ausente_levanta_value_error(self) -> None:
        """DataFrame sem BAIRRO → ValueError menciona 'BAIRRO' na mensagem."""
        df = pd.DataFrame({"NOME DO LOGRADOURO": ["Rua X"], "OUTRA": [1]})
        with pytest.raises(ValueError, match="BAIRRO"):
            validar_schema(df)

    def test_coluna_logradouro_ausente_levanta_value_error(self) -> None:
        """DataFrame sem NOME DO LOGRADOURO → ValueError menciona a coluna."""
        df = pd.DataFrame({"BAIRRO": ["Icaraí"], "OUTRA": [1]})
        with pytest.raises(ValueError, match="NOME DO LOGRADOURO"):
            validar_schema(df)

    def test_ambas_colunas_ausentes_mensagem_lista_todas(self) -> None:
        """DataFrame sem nenhuma coluna obrigatória → mensagem lista ambas."""
        df = pd.DataFrame({"COL_DESCONHECIDA": [1]})
        with pytest.raises(ValueError) as exc_info:
            validar_schema(df)
        msg = str(exc_info.value)
        assert "BAIRRO" in msg
        assert "NOME DO LOGRADOURO" in msg

    def test_mensagem_inclui_colunas_encontradas_para_diagnostico(self) -> None:
        """Mensagem de erro deve incluir as colunas encontradas (diagnóstico)."""
        df = pd.DataFrame({"COLUNA_ESTRANHA": [1], "OUTRA": [2]})
        with pytest.raises(ValueError, match="COLUNA_ESTRANHA"):
            validar_schema(df)

    def test_schema_completo_nao_levanta_excecao(self) -> None:
        """DataFrame com todas as colunas obrigatórias → sem exceção."""
        df = pd.DataFrame({col: ["valor"] for col in COLUNAS_REQUERIDAS})
        validar_schema(df)  # deve passar silenciosamente

    def test_schema_com_colunas_extras_aceito(self) -> None:
        """DataFrame com colunas extras além das obrigatórias → sem exceção."""
        df = pd.DataFrame(
            {
                "BAIRRO": ["Icaraí"],
                "NOME DO LOGRADOURO": ["Rua X"],
                "VALOR DA TRANSAÇÃO": [150000.0],
                "ANO DO PAGAMENTO DO ITBI": [2023],
            }
        )
        validar_schema(df)  # deve passar

    def test_colunas_requeridas_nao_e_vazio(self) -> None:
        """COLUNAS_REQUERIDAS deve ser não-vazio e conter as colunas de geocodificação."""
        assert len(COLUNAS_REQUERIDAS) >= 2
        assert "BAIRRO" in COLUNAS_REQUERIDAS
        assert "NOME DO LOGRADOURO" in COLUNAS_REQUERIDAS

    def test_colunas_customizadas_validadas(self) -> None:
        """Parâmetro 'colunas' permite sobrescrever as colunas verificadas."""
        df = pd.DataFrame({"BAIRRO": ["Icaraí"]})
        # Validação apenas de BAIRRO → sem exceção
        validar_schema(df, colunas=("BAIRRO",))
        # Validação de BAIRRO + COL_INEXISTENTE → ValueError
        with pytest.raises(ValueError, match="COL_INEXISTENTE"):
            validar_schema(df, colunas=("BAIRRO", "COL_INEXISTENTE"))


# ===========================================================================
# Parse HTML — contrato de descoberta da fonte
# ===========================================================================


class TestParseHtmlLinksCSV:
    """Valida que a estrutura HTML da fonte produz URLs válidas."""

    def test_retorna_dict_com_chaves_inteiras(self) -> None:
        """Todas as chaves do resultado devem ser int (anos), não strings."""
        with patch(
            "itbi.descoberta.requests.get",
            return_value=_mock_resp(_HTML_FIXTURE),
        ):
            resultado = descobrir_csv_urls(url=_URL_BASE)

        for chave in resultado:
            assert isinstance(chave, int), (
                f"Chave deveria ser int, mas é {type(chave).__name__!r}: {chave!r}"
            )

    def test_retorna_todos_os_anos_do_fixture(self) -> None:
        """Fixture com 5 anos → dicionário com exatamente esses 5 anos."""
        with patch(
            "itbi.descoberta.requests.get",
            return_value=_mock_resp(_HTML_FIXTURE),
        ):
            resultado = descobrir_csv_urls(url=_URL_BASE)

        assert set(resultado.keys()) == {2020, 2021, 2022, 2023, 2024}

    def test_valores_sao_urls_absolutas(self) -> None:
        """Cada URL no resultado deve ser absoluta (começa com http/https)."""
        with patch(
            "itbi.descoberta.requests.get",
            return_value=_mock_resp(_HTML_FIXTURE),
        ):
            resultado = descobrir_csv_urls(url=_URL_BASE)

        for ano, url in resultado.items():
            assert url.startswith("http"), (
                f"URL do ano {ano} deve ser absoluta, mas é: {url!r}"
            )

    def test_urls_seguem_padrao_nome_arquivo(self) -> None:
        """Cada URL deve conter 'transacoes_imobiliarias_<ano>.csv'."""
        with patch(
            "itbi.descoberta.requests.get",
            return_value=_mock_resp(_HTML_FIXTURE),
        ):
            resultado = descobrir_csv_urls(url=_URL_BASE)

        for ano, url in resultado.items():
            esperado = f"transacoes_imobiliarias_{ano}"
            assert esperado in url, (
                f"URL do ano {ano} não contém padrão esperado: {url!r}"
            )

    def test_urls_terminam_com_csv(self) -> None:
        """Cada URL deve terminar com extensão .csv."""
        with patch(
            "itbi.descoberta.requests.get",
            return_value=_mock_resp(_HTML_FIXTURE),
        ):
            resultado = descobrir_csv_urls(url=_URL_BASE)

        for ano, url in resultado.items():
            assert url.lower().endswith(".csv"), (
                f"URL do ano {ano} não termina em .csv: {url!r}"
            )

    def test_fixture_usa_div_entry_content(self) -> None:
        """Fixture usa div.entry-content (padrão WordPress); links devem ser encontrados."""
        assert 'class="entry-content"' in _HTML_FIXTURE
        with patch(
            "itbi.descoberta.requests.get",
            return_value=_mock_resp(_HTML_FIXTURE),
        ):
            resultado = descobrir_csv_urls(url=_URL_BASE)

        # Se o seletor principal funcionar, nenhum fallback deve ter sido necessário
        assert len(resultado) == 5


# ===========================================================================
# Separador e encoding — robustez
# ===========================================================================


class TestDetectaSeparadorEEncoding:
    """Robustez da consolidação frente a variações de separador e encoding."""

    def test_separador_virgula_sem_crash(self, tmp_path: Path) -> None:
        """CSV com ',' como separador é lido sem exceção."""
        arq = tmp_path / "t.csv"
        arq.write_text("BAIRRO,NOME DO LOGRADOURO\nIcaraí,Rua X\n", encoding="utf-8")

        df = carregar_e_consolidar([arq])

        assert "BAIRRO" in df.columns
        assert len(df) == 1

    def test_separador_ponto_e_virgula_sem_crash(self, tmp_path: Path) -> None:
        """CSV com ';' como separador é detectado automaticamente sem crash."""
        arq = tmp_path / "t.csv"
        arq.write_text("BAIRRO;NOME DO LOGRADOURO\nCentro;Rua Y\n", encoding="utf-8")

        df = carregar_e_consolidar([arq])

        assert "BAIRRO" in df.columns
        assert "NOME DO LOGRADOURO" in df.columns
        assert len(df) == 1

    def test_encoding_utf8_bom_sem_crash(self, tmp_path: Path) -> None:
        """CSV com BOM UTF-8 (exportação Excel) é lido sem crash."""
        arq = tmp_path / "t.csv"
        arq.write_bytes(b"\xef\xbb\xbf" + b"BAIRRO,NOME DO LOGRADOURO\nIcarai,Rua X\n")

        df = carregar_e_consolidar([arq])

        assert "BAIRRO" in df.columns
        assert len(df) == 1

    def test_encoding_latin1_sem_crash(self, tmp_path: Path) -> None:
        """CSV com encoding latin-1 (Windows-1252) é lido sem crash."""
        arq = tmp_path / "t.csv"
        # "São João" em latin-1: "ã" = 0xE3, inválido em UTF-8
        arq.write_bytes(
            "BAIRRO,NOME DO LOGRADOURO\nSão João,Rua Ação\n".encode("latin-1")
        )

        df = carregar_e_consolidar([arq])

        assert "BAIRRO" in df.columns
        assert len(df) == 1

    def test_separador_e_encoding_combinados_sem_crash(self, tmp_path: Path) -> None:
        """Separador ';' + encoding latin-1 simultaneamente → sem crash."""
        arq = tmp_path / "t.csv"
        arq.write_bytes(
            "BAIRRO;NOME DO LOGRADOURO\nSão João;Rua Ação\n".encode("latin-1")
        )

        df = carregar_e_consolidar([arq])

        assert "BAIRRO" in df.columns
        assert len(df) == 1

    def test_separador_ponto_e_virgula_preserva_dados(self, tmp_path: Path) -> None:
        """Com separador ';', dados não são particionados erroneamente."""
        arq = tmp_path / "t.csv"
        arq.write_text(
            "BAIRRO;NOME DO LOGRADOURO\nIcaraí;Rua Coronel Moreira César\n",
            encoding="utf-8",
        )

        df = carregar_e_consolidar([arq])

        # Cada campo deve ter sido lido como uma coluna separada
        assert df["BAIRRO"].notna().all()
        assert df["NOME DO LOGRADOURO"].notna().all()

    def test_encoding_utf8_sem_bom_aceito(self, tmp_path: Path) -> None:
        """UTF-8 puro (sem BOM) também é lido sem crash via utf-8-sig."""
        arq = tmp_path / "t.csv"
        # utf-8-sig lê UTF-8 com ou sem BOM
        arq.write_text("BAIRRO,NOME DO LOGRADOURO\nIcaraí,Rua X\n", encoding="utf-8")

        df = carregar_e_consolidar([arq])

        assert "BAIRRO" in df.columns
