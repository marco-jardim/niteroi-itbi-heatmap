"""
Etapa 1 — Descoberta dinâmica dos links CSV na página da SMF Niterói.

Uso standalone::

    python -m itbi.descoberta
    python -m itbi.descoberta --url https://...outro-endpoint...
    python -m itbi.descoberta --json   # saída em JSON compacto
"""

import json
import logging
import re
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from itbi.config import BASE_URL, CSV_URLS_FALLBACK, HEADERS

log = logging.getLogger(__name__)

# ===========================================================================
# Etapa 1 — Descoberta
# ===========================================================================


def descobrir_csv_urls(url: str = BASE_URL) -> dict[int, str]:
    """Acessa a página da SMF Niterói e extrai todos os links .csv presentes.

    Seletor CSS principal: ``div.entry-content`` (tema WordPress padrão).
    Padrão do href: ``*transacoes_imobiliarias_YYYY.csv``.

    Tenta os seguintes seletores de conteúdo em ordem::

        div.entry-content → div.post-content → main article → página inteira

    Args:
        url: URL da página com os links CSV. Padrão: :data:`~itbi.config.BASE_URL`.

    Returns:
        Dicionário ``{ano: url_absoluta}``.  Em caso de falha na requisição ou
        ausência de links, retorna :data:`~itbi.config.CSV_URLS_FALLBACK`.
    """
    log.info("[ETAPA 1] Acessando: %s", url)
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.warning("Falha ao acessar página: %s. Usando fallback.", e)
        return CSV_URLS_FALLBACK

    soup = BeautifulSoup(resp.text, "html.parser")

    # Tenta seletores CSS comuns de temas WordPress; cai na página inteira
    content = (
        soup.select_one("div.entry-content")
        or soup.select_one("div.post-content")
        or soup.select_one("main article")
        or soup
    )

    urls: dict[int, str] = {}
    for tag in content.find_all("a", href=True):
        href: str = tag["href"]
        match = re.search(r"transacoes_imobiliarias_(\d{4})\.csv", href, re.IGNORECASE)
        if match:
            ano = int(match.group(1))
            urls[ano] = urljoin(url, href)
            log.info("  [%d] %s", ano, urls[ano])

    if not urls:
        log.warning("Nenhum CSV encontrado dinamicamente — usando URLs hardcoded.")
        return CSV_URLS_FALLBACK

    log.info("  %d CSVs encontrados.", len(urls))
    return urls


# ===========================================================================
# Entrypoint standalone: python -m itbi.descoberta
# ===========================================================================


def _build_arg_parser():  # type: ignore[return]
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m itbi.descoberta",
        description="[ETAPA 1] Descobre URLs dos CSVs ITBI na página da SMF Niterói.",
    )
    parser.add_argument(
        "--url",
        default=BASE_URL,
        metavar="URL",
        help=f"URL da página de origem (padrão: {BASE_URL})",
    )
    parser.add_argument(
        "--json",
        dest="as_json",
        action="store_true",
        help="Imprime resultado como JSON compacto em vez de tabela.",
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
    csv_urls = descobrir_csv_urls(url=args.url)

    if args.as_json:
        print(json.dumps({str(k): v for k, v in sorted(csv_urls.items())}, indent=2))
    else:
        print(f"\n{'ANO':<6} {'URL'}")
        print("-" * 80)
        for ano, link in sorted(csv_urls.items()):
            print(f"{ano:<6} {link}")
        print(f"\nTotal: {len(csv_urls)} CSVs")

    sys.exit(0)
