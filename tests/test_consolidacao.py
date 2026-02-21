"""
Testes para itbi.consolidacao.

Cobre:
- Leitura de CSV com encoding UTF-8 BOM (Excel)
- Leitura de CSV com encoding latin-1 (Windows-1252)
- Limpeza de valores monetários: R$, pontos de milhar, vírgula decimal → float
- Coluna com valor não-numérico → NaN (sem crash)
- Normalização de nomes de colunas para UPPER (str.strip().str.upper())
- Normalização de texto: BAIRRO e NOME DO LOGRADOURO recebem Title Case
- Detecção automática de separador vírgula e ponto-e-vírgula
- Lista de arquivos vazia levanta ValueError com mensagem descritiva
- Múltiplos arquivos são concatenados corretamente
"""

from pathlib import Path

import pandas as pd
import pytest

from itbi.consolidacao import carregar_e_consolidar

# ===========================================================================
# Helpers
# ===========================================================================


def _escrever_utf8_bom(tmp_path: Path, content: str, name: str = "t.csv") -> Path:
    """Escreve CSV com BOM UTF-8 (como Excel exporta)."""
    path = tmp_path / name
    path.write_bytes(b"\xef\xbb\xbf" + content.encode("utf-8"))
    return path


def _escrever_latin1(tmp_path: Path, content: str, name: str = "t.csv") -> Path:
    """Escreve CSV com encoding latin-1."""
    path = tmp_path / name
    path.write_bytes(content.encode("latin-1"))
    return path


# ===========================================================================
# Encoding: UTF-8 BOM
# ===========================================================================


def test_carregar_encoding_utf8_bom(tmp_path: Path) -> None:
    """CSV com BOM UTF-8 é lido corretamente sem erro de encoding."""
    content = "BAIRRO,NOME DO LOGRADOURO\nIcaraí,Rua Coronel Moreira César\n"
    arq = _escrever_utf8_bom(tmp_path, content)

    df = carregar_e_consolidar([arq])

    assert "BAIRRO" in df.columns
    assert "NOME DO LOGRADOURO" in df.columns
    assert len(df) == 1


def test_carregar_encoding_utf8_bom_preserva_acentos(tmp_path: Path) -> None:
    """Caracteres acentuados UTF-8 (ç, ã, é) são preservados após leitura."""
    content = "BAIRRO,NOME DO LOGRADOURO\nSão Domingos,Rua das Acácias\n"
    arq = _escrever_utf8_bom(tmp_path, content)

    df = carregar_e_consolidar([arq])

    # Após title case: "São Domingos" mantém acentos
    assert "ã" in df["BAIRRO"].iloc[0] or "A" in df["BAIRRO"].iloc[0]
    assert len(df) == 1


# ===========================================================================
# Encoding: latin-1
# ===========================================================================


def test_carregar_encoding_latin1(tmp_path: Path) -> None:
    """CSV com encoding latin-1 é lido sem crash (fallback de encoding)."""
    # "ã" em latin-1 = 0xE3, que é byte inválido em UTF-8 puro
    content = "BAIRRO,NOME DO LOGRADOURO\nSão Francisco,Rua São João\n"
    arq = _escrever_latin1(tmp_path, content)

    df = carregar_e_consolidar([arq])

    assert "BAIRRO" in df.columns
    assert len(df) == 1


def test_carregar_encoding_latin1_acentos_preservados(tmp_path: Path) -> None:
    """Caracteres acentuados latin-1 são preservados corretamente após leitura."""
    content = "BAIRRO,NOME DO LOGRADOURO\nIcaraí,Rua Ação\n"
    arq = _escrever_latin1(tmp_path, content)

    df = carregar_e_consolidar([arq])

    # Após title case, bairro deve conter a letra "í"
    assert "í" in df["BAIRRO"].iloc[0]


# ===========================================================================
# Limpeza de valores monetários
# ===========================================================================


def test_limpeza_valores_monetarios_remove_rs_e_pontos(tmp_path: Path) -> None:
    """R$, pontos de milhar e vírgula decimal são convertidos para float."""
    # Valor com vírgulas deve estar entre aspas no CSV para não confundir o parser
    content = (
        'BAIRRO,NOME DO LOGRADOURO,VALOR DA TRANSAÇÃO\nIcaraí,Rua X,"R$ 1.234.567,89"\n'
    )
    arq = _escrever_utf8_bom(tmp_path, content)

    df = carregar_e_consolidar([arq])

    assert pd.api.types.is_float_dtype(df["VALOR DA TRANSAÇÃO"])
    assert df["VALOR DA TRANSAÇÃO"].iloc[0] == pytest.approx(1_234_567.89)


def test_limpeza_valores_monetarios_sem_rs(tmp_path: Path) -> None:
    """Valor numérico sem prefixo R$ também é convertido para float."""
    content = "BAIRRO,VALOR DA TRANSAÇÃO\nCentro,1500\n"
    arq = _escrever_utf8_bom(tmp_path, content)

    df = carregar_e_consolidar([arq])

    assert df["VALOR DA TRANSAÇÃO"].iloc[0] == pytest.approx(1500.0)


def test_limpeza_quantidade_convertida_para_float(tmp_path: Path) -> None:
    """Coluna QUANTIDADE DE TRANSAÇÕES é convertida para numérico via pd.to_numeric."""
    content = "BAIRRO,QUANTIDADE DE TRANSAÇÕES\nCentro,10\n"
    arq = _escrever_utf8_bom(tmp_path, content)

    df = carregar_e_consolidar([arq])

    # pd.to_numeric produz int64 para inteiros puros e float64 quando há NaN;
    # ambos são numéricos — o contrato é que NÃO seja object/string.
    assert pd.api.types.is_numeric_dtype(df["QUANTIDADE DE TRANSAÇÕES"])
    assert df["QUANTIDADE DE TRANSAÇÕES"].iloc[0] == 10


def test_limpeza_valor_invalido_vira_nan(tmp_path: Path) -> None:
    """Valor não-numérico em coluna monetária vira NaN sem lançar exceção."""
    content = "BAIRRO,VALOR DA TRANSAÇÃO\nCentro,N/A\n"
    arq = _escrever_utf8_bom(tmp_path, content)

    df = carregar_e_consolidar([arq])

    assert pd.isna(df["VALOR DA TRANSAÇÃO"].iloc[0])


# ===========================================================================
# Normalização de nomes de colunas (UPPER)
# ===========================================================================


def test_colunas_normalizadas_para_upper(tmp_path: Path) -> None:
    """Colunas em minúsculas são normalizadas para UPPER após leitura."""
    content = "bairro,nome do logradouro\nicaraí,rua x\n"
    arq = _escrever_utf8_bom(tmp_path, content)

    df = carregar_e_consolidar([arq])

    assert "BAIRRO" in df.columns
    assert "NOME DO LOGRADOURO" in df.columns
    # Não deve haver versão minúscula
    assert "bairro" not in df.columns
    assert "nome do logradouro" not in df.columns


def test_colunas_com_espacos_extras_normalizadas(tmp_path: Path) -> None:
    """Espaços extras ao redor dos nomes de colunas são removidos (str.strip)."""
    content = " BAIRRO , NOME DO LOGRADOURO \nIcaraí,Rua X\n"
    arq = _escrever_utf8_bom(tmp_path, content)

    df = carregar_e_consolidar([arq])

    assert "BAIRRO" in df.columns
    assert "NOME DO LOGRADOURO" in df.columns


# ===========================================================================
# Normalização de texto (Title Case)
# ===========================================================================


def test_normalizacao_bairro_title_case(tmp_path: Path) -> None:
    """Coluna BAIRRO recebe Title Case: ICARAÍ → Icaraí."""
    content = "BAIRRO,NOME DO LOGRADOURO\nICARAÍ,RUA X\n"
    arq = _escrever_utf8_bom(tmp_path, content)

    df = carregar_e_consolidar([arq])

    assert df["BAIRRO"].iloc[0] == "Icaraí"


def test_normalizacao_logradouro_title_case(tmp_path: Path) -> None:
    """Coluna NOME DO LOGRADOURO recebe Title Case: RUA DAS FLORES → Rua Das Flores."""
    content = "BAIRRO,NOME DO LOGRADOURO\nCentro,RUA DAS FLORES\n"
    arq = _escrever_utf8_bom(tmp_path, content)

    df = carregar_e_consolidar([arq])

    assert df["NOME DO LOGRADOURO"].iloc[0] == "Rua Das Flores"


def test_normalizacao_strip_espacos_texto(tmp_path: Path) -> None:
    """Espaços ao redor dos valores de texto são removidos antes do Title Case."""
    content = "BAIRRO,NOME DO LOGRADOURO\n  CENTRO  ,  RUA X  \n"
    arq = _escrever_utf8_bom(tmp_path, content)

    df = carregar_e_consolidar([arq])

    # strip garante que não há espaços no início/fim
    assert df["BAIRRO"].iloc[0] == df["BAIRRO"].iloc[0].strip()


# ===========================================================================
# Separador automático
# ===========================================================================


def test_separador_virgula_lido_corretamente(tmp_path: Path) -> None:
    """CSV com separador ',' (padrão) é detectado e lido com todas as colunas."""
    content = "BAIRRO,NOME DO LOGRADOURO\nCentro,Rua Y\n"
    arq = _escrever_utf8_bom(tmp_path, content)

    df = carregar_e_consolidar([arq])

    assert "BAIRRO" in df.columns
    assert "NOME DO LOGRADOURO" in df.columns
    assert len(df) == 1


def test_separador_ponto_e_virgula_detectado(tmp_path: Path) -> None:
    """CSV com separador ';' é detectado automaticamente sem configuração manual."""
    content = "BAIRRO;NOME DO LOGRADOURO\nIcaraí;Rua X\n"
    arq = _escrever_utf8_bom(tmp_path, content)

    df = carregar_e_consolidar([arq])

    assert "BAIRRO" in df.columns
    assert "NOME DO LOGRADOURO" in df.columns
    assert len(df) == 1


# ===========================================================================
# Erro: lista de arquivos vazia
# ===========================================================================


def test_lista_vazia_levanta_value_error() -> None:
    """Lista de arquivos vazia levanta ValueError com mensagem descritiva."""
    with pytest.raises(ValueError, match="Nenhum CSV carregado"):
        carregar_e_consolidar([])


# ===========================================================================
# Múltiplos arquivos
# ===========================================================================


def test_consolida_dois_arquivos(tmp_path: Path) -> None:
    """Dois CSVs são concatenados; DataFrame final tem linhas de ambos."""
    content1 = "BAIRRO,NOME DO LOGRADOURO\nIcaraí,Rua A\n"
    content2 = "BAIRRO,NOME DO LOGRADOURO\nCentro,Rua B\n"
    arq1 = _escrever_utf8_bom(tmp_path, content1, "t1.csv")
    arq2 = _escrever_utf8_bom(tmp_path, content2, "t2.csv")

    df = carregar_e_consolidar([arq1, arq2])

    assert len(df) == 2
    assert set(df["BAIRRO"].tolist()) == {"Icaraí", "Centro"}


def test_consolida_ignorar_arquivo_invalido(tmp_path: Path) -> None:
    """Arquivo corrompido é ignorado; pipeline continua com os demais."""
    arq_valido = _escrever_utf8_bom(
        tmp_path,
        "BAIRRO,NOME DO LOGRADOURO\nIcaraí,Rua X\n",
        "valido.csv",
    )
    arq_invalido = tmp_path / "invalido.csv"
    # Arquivo binário não legível como CSV
    arq_invalido.write_bytes(b"\x00\x01\x02\x03\xff\xfe")

    # O arquivo inválido deve ser ignorado (log de erro), mas o válido processado
    df = carregar_e_consolidar([arq_valido, arq_invalido])

    assert len(df) == 1
    assert "BAIRRO" in df.columns
