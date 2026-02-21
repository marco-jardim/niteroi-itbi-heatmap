"""
Etapa 2 — Download dos CSVs anuais com cache simples.

Uso standalone::

    python -m itbi.download
    python -m itbi.download --anos 2022 2023
    python -m itbi.download --force          # re-baixa mesmo se arquivo existir
    python -m itbi.download --anos 2024 --force
"""

import logging
import time
from pathlib import Path

import requests

from itbi.config import DATA_DIR, HEADERS

log = logging.getLogger(__name__)

# ===========================================================================
# Etapa 2 — Download
# ===========================================================================


def baixar_csvs(
    csv_urls: dict[int, str],
    destino: Path = DATA_DIR,
    anos: list[int] | None = None,
    force: bool = False,
) -> list[Path]:
    """Faz download de cada CSV anual e salva em *destino*.

    Cache simples: pula se o arquivo já existir, a menos que ``force=True``.

    Args:
        csv_urls: Dicionário ``{ano: url}`` com os CSVs a baixar.
        destino:  Diretório destino. Criado automaticamente se não existir.
        anos:     Lista de anos a filtrar.  ``None`` baixa todos os disponíveis.
        force:    Se ``True``, re-baixa mesmo que o arquivo já exista no disco.

    Returns:
        Lista dos :class:`~pathlib.Path` dos arquivos presentes no disco após o
        processo (incluindo os que já existiam e foram pulados).
    """
    log.info("[ETAPA 2] Baixando CSVs...")
    destino.mkdir(parents=True, exist_ok=True)

    urls_filtradas: dict[int, str] = (
        {ano: url for ano, url in csv_urls.items() if ano in anos}
        if anos is not None
        else csv_urls
    )

    if anos is not None:
        anos_ausentes = set(anos) - set(urls_filtradas)
        if anos_ausentes:
            log.warning(
                "Anos solicitados sem URL disponível: %s",
                sorted(anos_ausentes),
            )

    arquivos: list[Path] = []

    for ano, url in sorted(urls_filtradas.items()):
        arquivo = destino / f"transacoes_imobiliarias_{ano}.csv"

        if arquivo.exists() and not force:
            log.info("  [%d] Já existe, pulando. (use --force para re-baixar)", ano)
            arquivos.append(arquivo)
            continue

        if arquivo.exists() and force:
            log.info("  [%d] Forçando re-download: %s", ano, url)
        else:
            log.info("  [%d] Baixando: %s", ano, url)

        try:
            resp = requests.get(url, headers=HEADERS, timeout=60, stream=True)
            resp.raise_for_status()
            with arquivo.open("wb") as fh:
                for chunk in resp.iter_content(chunk_size=8192):
                    fh.write(chunk)
            log.info("  [%d] Salvo: %s", ano, arquivo)
            arquivos.append(arquivo)
            time.sleep(1)
        except requests.RequestException as e:
            log.error("  [%d] Falha no download: %s", ano, e)

    return arquivos


# ===========================================================================
# Entrypoint standalone: python -m itbi.download
# ===========================================================================


def _build_arg_parser():  # type: ignore[return]
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m itbi.download",
        description=(
            "[ETAPA 2] Baixa CSVs ITBI anuais da SMF Niterói para o diretório de dados."
        ),
    )
    parser.add_argument(
        "--anos",
        nargs="+",
        type=int,
        metavar="ANO",
        help="Anos específicos a baixar (ex: --anos 2022 2023). Padrão: todos.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-baixa mesmo que o arquivo já exista no disco.",
    )
    parser.add_argument(
        "--destino",
        type=Path,
        default=DATA_DIR,
        metavar="DIR",
        help=f"Diretório destino (padrão: {DATA_DIR})",
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

    # Importa descoberta aqui para evitar dependência circular no topo do módulo
    from itbi.descoberta import descobrir_csv_urls

    csv_urls = descobrir_csv_urls()
    arquivos = baixar_csvs(
        csv_urls=csv_urls,
        destino=args.destino,
        anos=args.anos,
        force=args.force,
    )

    print(f"\n{len(arquivos)} arquivo(s) disponível(is):")
    for arq in arquivos:
        print(f"  {arq}")

    sys.exit(0 if arquivos else 1)
