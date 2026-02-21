"""
Etapa 3 — Consolidação e limpeza dos CSVs anuais.

Uso standalone::

    python -m itbi.consolidacao
    python -m itbi.consolidacao --destino data/itbi_niteroi
"""

import logging
from pathlib import Path

import pandas as pd

from itbi.config import DATA_DIR

log = logging.getLogger(__name__)

# Colunas cujos valores devem ser convertidos para numérico (R$, pontos, vírgulas)
_COLUNAS_NUMERICAS_CHAVE: tuple[str, ...] = ("VALOR", "ÁREA", "QUANTIDADE")

# Colunas de texto que recebem normalização de capitalização
_COLUNAS_TEXTO: tuple[str, ...] = ("BAIRRO", "NOME DO LOGRADOURO")

# Colunas mínimas obrigatórias para que o pipeline funcione
COLUNAS_REQUERIDAS: tuple[str, ...] = ("BAIRRO", "NOME DO LOGRADOURO")

# ===========================================================================
# Etapa 3 — Consolidação
# ===========================================================================


def carregar_e_consolidar(arquivos: list[Path]) -> pd.DataFrame:
    """Lê e consolida todos os CSVs em um único :class:`~pandas.DataFrame`.

    Estratégia de encoding: tenta ``utf-8-sig`` (BOM Windows/Excel) primeiro;
    cai em ``latin-1`` se encontrar :exc:`UnicodeDecodeError`.

    Separador: detectado automaticamente (vírgula ou ponto-e-vírgula) via
    ``sep=None, engine='python'``.

    Limpeza de valores monetários: remove ``R$``, pontos de milhar e espaços;
    troca vírgula decimal por ponto; aplica :func:`pandas.to_numeric` com
    ``errors='coerce'`` para não travar em dados sujos.

    Args:
        arquivos: Lista de :class:`~pathlib.Path` dos CSVs a processar.

    Returns:
        DataFrame consolidado e limpo.

    Raises:
        ValueError: Se *arquivos* for vazio ou nenhum CSV for legível.
    """
    log.info("[ETAPA 3] Consolidando e limpando dados...")
    frames: list[pd.DataFrame] = []

    for arq in arquivos:
        df = _ler_csv_com_fallback(arq)
        if df is None:
            continue

        df.columns = df.columns.str.strip().str.upper()
        log.info("  %s: %d linhas, colunas: %s", arq.name, len(df), list(df.columns))
        frames.append(df)

    if not frames:
        raise ValueError(
            "Nenhum CSV carregado. Verifique se os arquivos existem e são legíveis."
        )

    df_consolidado = pd.concat(frames, ignore_index=True)
    df_consolidado.dropna(how="all", inplace=True)

    df_consolidado = _limpar_numericos(df_consolidado)
    df_consolidado = _normalizar_texto(df_consolidado)

    log.info("  Total consolidado: %d linhas", len(df_consolidado))
    return df_consolidado


def salvar_consolidado(df: pd.DataFrame, destino: Path = DATA_DIR) -> Path:
    """Salva o DataFrame consolidado como ``consolidado.csv`` em *destino*.

    Usa encoding ``utf-8-sig`` (BOM) para compatibilidade com Excel.

    Args:
        df:      DataFrame a salvar.
        destino: Diretório destino. Criado automaticamente se não existir.

    Returns:
        :class:`~pathlib.Path` do arquivo salvo.
    """
    destino.mkdir(parents=True, exist_ok=True)
    saida = destino / "consolidado.csv"
    df.to_csv(saida, index=False, encoding="utf-8-sig")
    log.info("  Consolidado salvo: %s (%d linhas)", saida, len(df))
    return saida


# ===========================================================================
# Helpers privados
# ===========================================================================


def _ler_csv_com_fallback(arq: Path) -> pd.DataFrame | None:
    """Lê um CSV tentando UTF-8 BOM e depois latin-1.

    Args:
        arq: Caminho do arquivo CSV.

    Returns:
        DataFrame lido, ou ``None`` se a leitura falhar completamente.
    """
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(arq, encoding=encoding, sep=None, engine="python")
        except UnicodeDecodeError:
            continue
        except Exception as e:  # noqa: BLE001
            log.error("  Falha ao ler %s: %s", arq.name, e)
            return None
    log.error("  Não foi possível decodificar %s com UTF-8 nem latin-1.", arq.name)
    return None


def _limpar_numericos(df: pd.DataFrame) -> pd.DataFrame:
    """Remove formatação monetária e converte colunas numéricas.

    Aplica a todas as colunas que contêm palavras-chave em
    :data:`_COLUNAS_NUMERICAS_CHAVE`.
    """
    for col in df.columns:
        if any(chave in col for chave in _COLUNAS_NUMERICAS_CHAVE):
            df[col] = (
                df[col]
                .astype(str)
                .str.replace(r"[R$\.\s]", "", regex=True)
                .str.replace(",", ".", regex=False)
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _normalizar_texto(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza colunas de texto: strip + Title Case.

    Aplica às colunas listadas em :data:`_COLUNAS_TEXTO` que existam no DataFrame.
    """
    for col in _COLUNAS_TEXTO:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()
    return df


# ===========================================================================
# Validação de schema
# ===========================================================================


def validar_schema(
    df: pd.DataFrame,
    colunas: tuple[str, ...] = COLUNAS_REQUERIDAS,
) -> None:
    """Valida que o DataFrame contém as colunas mínimas obrigatórias.

    Args:
        df:      DataFrame a validar.
        colunas: Colunas que devem estar presentes. Padrão:
                 :data:`COLUNAS_REQUERIDAS`.

    Raises:
        ValueError: Se qualquer coluna obrigatória estiver ausente, com lista
                    acionável das colunas faltantes e das colunas encontradas.
    """
    ausentes = [c for c in colunas if c not in df.columns]
    if ausentes:
        encontradas = sorted(df.columns.tolist())
        raise ValueError(
            f"Schema inválido — colunas ausentes: {ausentes}. "
            f"Colunas encontradas: {encontradas}"
        )


# ===========================================================================
# Entrypoint standalone: python -m itbi.consolidacao
# ===========================================================================


def _build_arg_parser():  # type: ignore[return]
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m itbi.consolidacao",
        description=(
            "[ETAPA 3] Consolida e limpa os CSVs anuais do DATA_DIR, "
            "gerando consolidado.csv."
        ),
    )
    parser.add_argument(
        "--destino",
        type=Path,
        default=DATA_DIR,
        metavar="DIR",
        help=f"Diretório com os CSVs de entrada e destino do consolidado.csv "
        f"(padrão: {DATA_DIR})",
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
    destino: Path = args.destino

    # Coleta CSVs anuais existentes no diretório destino
    csvs = sorted(destino.glob("transacoes_imobiliarias_*.csv"))

    if not csvs:
        log.error(
            "Nenhum CSV encontrado em '%s'. "
            "Execute 'python -m itbi.download' primeiro.",
            destino,
        )
        sys.exit(1)

    log.info("CSVs encontrados: %s", [c.name for c in csvs])

    try:
        df = carregar_e_consolidar(csvs)
    except ValueError as e:
        log.error("%s", e)
        sys.exit(1)

    saida = salvar_consolidado(df, destino=destino)
    print(f"\nConsolidado salvo: {saida} ({len(df)} linhas)")
    sys.exit(0)
