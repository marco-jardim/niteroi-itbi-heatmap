"""
CLI unificado do pipeline ITBI Niterói.

Subcomandos disponíveis::

    itbi run          [--anos ...] [--skip-download] [--skip-geo] [--no-markers]
                      [--choropleth-geojson PATH] [--choropleth-key PROP]
    itbi descobrir    [--json]
    itbi baixar       [--anos ...] [--force]
    itbi consolidar
    itbi geocodificar [--reset-cache] [--limite N]
    itbi mapa         [--no-markers] [--output PATH]
                      [--choropleth-geojson PATH] [--choropleth-key PROP]
    itbi insights     [--input CSV] [--output JSON]
    itbi backtest     [--input CSV]
    itbi status
    itbi limpar       [--tudo --confirmar]
"""

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Callable

from itbi.config import (
    DATA_DIR,
    DATA_JSON,
    DOCS_DIR,
    GEOCACHE_CSV,
    OUTPUT_HTML,
)

log = logging.getLogger(__name__)


# ===========================================================================
# Logging
# ===========================================================================


def _setup_logging(verbose: bool = False) -> None:
    """Configura logging do pipeline."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )


# ===========================================================================
# Subcomando: run — pipeline completo
# ===========================================================================


def cmd_run(args: argparse.Namespace) -> int:
    """Pipeline completo: descobrir → baixar → consolidar → geocodificar → mapa."""
    import pandas as pd

    from itbi.consolidacao import carregar_e_consolidar, salvar_consolidado
    from itbi.descoberta import descobrir_csv_urls
    from itbi.download import baixar_csvs
    from itbi.geocodificacao import geocodificar
    from itbi.heatmap import gerar_heatmap

    log.info("=" * 60)
    log.info("ITBI Niterói — Pipeline completo")
    log.info("=" * 60)

    # ------------------------------------------------------------------
    # [ETAPA 1-2] Descobrir + Baixar
    # ------------------------------------------------------------------
    if not args.skip_download:
        csv_urls = descobrir_csv_urls()
        anos: list[int] | None = args.anos if args.anos else None
        arquivos = baixar_csvs(csv_urls, anos=anos)
    else:
        log.info("[ETAPA 1-2] Pulando download (--skip-download)")
        arquivos = sorted(DATA_DIR.glob("transacoes_imobiliarias_*.csv"))
        if not arquivos:
            log.error(
                "Nenhum CSV encontrado em '%s'. Execute sem --skip-download.", DATA_DIR
            )
            return 1

    if not arquivos:
        log.error("Nenhum CSV disponível. Abortando.")
        return 1

    # ------------------------------------------------------------------
    # [ETAPA 3] Consolidar
    # ------------------------------------------------------------------
    df = carregar_e_consolidar(arquivos)
    salvar_consolidado(df)

    # ------------------------------------------------------------------
    # [ETAPA 4] Geocodificar
    # ------------------------------------------------------------------
    if not args.skip_geo:
        geocoder = getattr(args, "geocoder", "nominatim")
        df_geo = geocodificar(df, geocoder=geocoder)
        df_geo.to_csv(
            DATA_DIR / "consolidado_geo.csv",
            index=False,
            encoding="utf-8-sig",
        )
    else:
        log.info("[ETAPA 4] Pulando geocodificação (--skip-geo)")
        geo_path = DATA_DIR / "consolidado_geo.csv"
        if not geo_path.exists():
            log.error(
                "Arquivo não encontrado: '%s'. Execute sem --skip-geo primeiro.",
                geo_path,
            )
            return 1
        df_geo = pd.read_csv(geo_path)

    # ------------------------------------------------------------------
    # [ETAPA 5] Mapa
    # ------------------------------------------------------------------
    geojson = getattr(args, "choropleth_geojson", None)
    choropleth_key: str = getattr(args, "choropleth_key", "nome") or "nome"
    gerar_heatmap(
        df_geo,
        incluir_marcadores=not args.no_markers,
        geojson_bairros=Path(geojson) if geojson else None,
        choropleth_key=choropleth_key,
    )

    # ------------------------------------------------------------------
    # [ETAPA 6] Insights (opcional — requer consolidado_geo.csv)
    # ------------------------------------------------------------------
    geo_csv = DATA_DIR / "consolidado_geo.csv"
    if geo_csv.exists():
        try:
            from itbi.insights import gerar_insights

            gerar_insights(consolidado_geo_csv=geo_csv)
        except (ValueError, FileNotFoundError) as exc:
            log.warning("Insights não gerados: %s", exc)
    else:
        log.info("[ETAPA 6] Pulando insights (consolidado_geo.csv não encontrado)")

    log.info("=" * 60)
    log.info("Pipeline concluído!")
    log.info("  Heatmap: %s", OUTPUT_HTML.resolve())
    log.info("  JSON:    %s", DATA_JSON.resolve())
    log.info("=" * 60)
    return 0


# ===========================================================================
# Subcomando: descobrir
# ===========================================================================


def cmd_descobrir(args: argparse.Namespace) -> int:
    """Imprime URLs dos CSVs encontrados na página da SMF."""
    from itbi.descoberta import descobrir_csv_urls

    urls = descobrir_csv_urls()
    if args.json:
        print(json.dumps({str(k): v for k, v in sorted(urls.items())}, indent=2))
    else:
        print(f"\n{'ANO':<6}  URL")
        print("-" * 80)
        for ano, url in sorted(urls.items()):
            print(f"{ano:<6}  {url}")
        print(f"\nTotal: {len(urls)} CSVs encontrados.")
    return 0


# ===========================================================================
# Subcomando: baixar
# ===========================================================================


def cmd_baixar(args: argparse.Namespace) -> int:
    """Baixa CSVs anuais da SMF."""
    from itbi.descoberta import descobrir_csv_urls
    from itbi.download import baixar_csvs

    csv_urls = descobrir_csv_urls()
    anos: list[int] | None = args.anos if args.anos else None
    arquivos = baixar_csvs(csv_urls, anos=anos, force=args.force)
    log.info("  %d arquivo(s) disponível(is).", len(arquivos))
    return 0


# ===========================================================================
# Subcomando: consolidar
# ===========================================================================


def cmd_consolidar(args: argparse.Namespace) -> int:
    """Lê os CSVs baixados e gera consolidado.csv."""
    from itbi.consolidacao import carregar_e_consolidar, salvar_consolidado

    arquivos = sorted(DATA_DIR.glob("transacoes_imobiliarias_*.csv"))
    if not arquivos:
        log.error(
            "Nenhum CSV encontrado em '%s'. Execute 'itbi baixar' primeiro.", DATA_DIR
        )
        return 1

    df = carregar_e_consolidar(arquivos)
    saida = salvar_consolidado(df)
    print(f"Consolidado: {saida} ({len(df)} linhas)")
    return 0


# ===========================================================================
# Subcomando: geocodificar
# ===========================================================================


def cmd_geocodificar(args: argparse.Namespace) -> int:
    """Geocodifica endereços do consolidado.csv."""
    import pandas as pd

    from itbi.geocodificacao import geocodificar

    csv_path = DATA_DIR / "consolidado.csv"
    if not csv_path.exists():
        log.error(
            "Arquivo não encontrado: '%s'. Execute 'itbi consolidar' primeiro.",
            csv_path,
        )
        return 1

    df = pd.read_csv(csv_path)
    geocoder = getattr(args, "geocoder", "nominatim")
    df_geo = geocodificar(
        df,
        reset_cache=args.reset_cache,
        limite=args.limite,
        geocoder=geocoder,
    )
    saida = DATA_DIR / "consolidado_geo.csv"
    df_geo.to_csv(saida, index=False, encoding="utf-8-sig")
    print(f"Geocodificado: {saida} ({len(df_geo)} linhas)")
    return 0


# ===========================================================================
# Subcomando: mapa
# ===========================================================================


def cmd_mapa(args: argparse.Namespace) -> int:
    """Gera index.html e itbi_geo.json."""
    import pandas as pd

    from itbi.heatmap import gerar_heatmap

    geo_path = DATA_DIR / "consolidado_geo.csv"
    if not geo_path.exists():
        log.error(
            "Arquivo não encontrado: '%s'. Execute 'itbi geocodificar' primeiro.",
            geo_path,
        )
        return 1

    output_path = Path(args.output) if args.output else OUTPUT_HTML
    geojson = getattr(args, "choropleth_geojson", None)
    choropleth_key: str = getattr(args, "choropleth_key", "nome") or "nome"

    df = pd.read_csv(geo_path)
    gerar_heatmap(
        df,
        output_path=output_path,
        incluir_marcadores=not args.no_markers,
        geojson_bairros=Path(geojson) if geojson else None,
        choropleth_key=choropleth_key,
    )
    print(f"Mapa gerado: {output_path}")
    return 0


# ===========================================================================
# Subcomando: insights
# ===========================================================================


def cmd_insights(args: argparse.Namespace) -> int:
    """Gera insights de valorização e joias escondidas."""
    from itbi.insights import INSIGHTS_JSON, gerar_insights

    input_csv = Path(args.input) if args.input else DATA_DIR / "consolidado_geo.csv"
    output_json = Path(args.output) if args.output else INSIGHTS_JSON

    if not input_csv.exists():
        log.error(
            "Arquivo não encontrado: '%s'. Execute 'itbi geocodificar' primeiro.",
            input_csv,
        )
        return 1

    try:
        out = gerar_insights(consolidado_geo_csv=input_csv, output_json=output_json)
        print(f"Insights gerados: {out}")
    except ValueError as e:
        log.error("Erro ao gerar insights: %s", e)
        return 1
    return 0


# ===========================================================================
# Subcomando: backtest
# ===========================================================================


def cmd_backtest(args: argparse.Namespace) -> int:
    """Executa mini-backtest de calibração de pesos/thresholds."""
    from itbi.backtest import (
        BACKTEST_BEST_JSON,
        BACKTEST_REPORT_JSON,
        executar_backtest,
    )

    input_csv = Path(args.input) if args.input else DATA_DIR / "consolidado_geo.csv"

    if not input_csv.exists():
        log.error(
            "Arquivo não encontrado: '%s'. Execute 'itbi geocodificar' primeiro.",
            input_csv,
        )
        return 1

    try:
        rpt, bst = executar_backtest(
            consolidado_geo_csv=input_csv,
            report_json=BACKTEST_REPORT_JSON,
            best_json=BACKTEST_BEST_JSON,
        )
        print(f"Relatório: {rpt}")
        print(f"Melhor configuração: {bst}")
    except (ValueError, FileNotFoundError) as e:
        log.error("Erro no backtest: %s", e)
        return 1
    return 0


def cmd_normalizar_enderecos(args: argparse.Namespace) -> int:
    """Normaliza endereços via LLM (Fireworks AI) e salva JSON estruturado."""
    from itbi.normalizacao_llm import (
        normalizar_enderecos_llm,
        ENDERECOS_NORM_JSON,
        carregar_normalizados,
    )
    from itbi.config import DATA_DIR
    from itbi.consolidacao import carregar_e_consolidar
    from itbi.geocodificacao import _montar_endereco

    consolidado = DATA_DIR / "consolidado.csv"
    if not consolidado.exists():
        log.error("Arquivo não encontrado: %s — rode `itbi consolidar` antes.", consolidado)
        return 1

    df = carregar_e_consolidar([consolidado])
    df["ENDERECO"] = df.apply(_montar_endereco, axis=1)

    output = Path(args.output) if getattr(args, "output", None) else ENDERECOS_NORM_JSON

    resultado = normalizar_enderecos_llm(
        df,
        output_path=output,
        api_key=getattr(args, "api_key", None) or "",
        batch_size=getattr(args, "batch_size", 50),
    )
    log.info("Normalização concluída: %d endereços → %s", len(resultado), output)
    return 0


def cmd_street_map(args: argparse.Namespace) -> int:
    """Gera mapa de ruas coloridas por score de valorização."""
    from itbi.street_map import gerar_street_map
    from itbi.consolidacao import carregar_e_consolidar
    from itbi.config import DATA_DIR

    geo_path = Path(args.input) if args.input else DATA_DIR / "consolidado_geo.csv"
    if not geo_path.exists():
        log.error("Arquivo geocodificado não encontrado: %s. Execute 'itbi geocodificar' primeiro.", geo_path)
        return 1

    insights_path = Path(args.insights) if args.insights else Path("docs/data/itbi_insights.json")

    try:
        import pandas as pd
        df_geo = pd.read_csv(geo_path, encoding="utf-8-sig")
    except UnicodeDecodeError:
        import pandas as pd
        df_geo = pd.read_csv(geo_path, encoding="latin-1")

    output_path = Path(args.output) if args.output else Path("docs/street_map.html")

    try:
        gerar_street_map(
            df_geo=df_geo,
            insights_path=insights_path,
            output_path=output_path,
            score_col=args.score,
            janela=args.janela,
        )
        log.info("Street map salvo: %s", output_path)
    except (ValueError, FileNotFoundError, ImportError) as e:
        log.error("Erro ao gerar street map: %s", e)
        return 1
    return 0


# ===========================================================================
# Subcomando: status
# ===========================================================================


def _fmt_mod(path: Path) -> str:
    """Formata timestamp de modificação de um arquivo."""
    ts = path.stat().st_mtime
    return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _fmt_size(path: Path) -> str:
    """Formata tamanho de arquivo de forma legível."""
    n = path.stat().st_size
    if n >= 1024 * 1024:
        return f"{n / 1024 / 1024:.1f} MB"
    if n >= 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n} B"


def _count_csv_rows(path: Path) -> int:
    """Conta linhas de um CSV eficientemente (sem ler para memória)."""
    with open(path, "rb") as fh:
        return max(0, sum(1 for _ in fh) - 1)  # -1 para cabeçalho


def cmd_status(args: argparse.Namespace) -> int:
    """Exibe estado dos artefatos do pipeline."""
    artefatos: list[tuple[str, Path, bool]] = [
        # (nome, caminho, contar_linhas)
        ("CSVs anuais", DATA_DIR, False),  # tratado separado
        ("consolidado.csv", DATA_DIR / "consolidado.csv", True),
        ("consolidado_geo.csv", DATA_DIR / "consolidado_geo.csv", True),
        ("geocache.csv", GEOCACHE_CSV, True),
        ("docs/index.html", OUTPUT_HTML, False),
        ("docs/data/itbi_geo.json", DATA_JSON, False),
    ]

    # CSVs anuais
    csvs = sorted(DATA_DIR.glob("transacoes_imobiliarias_*.csv"))
    csv_anos = [int(c.stem.split("_")[-1]) for c in csvs] if csvs else []

    print(
        f"\n{'Artefato':<26}  {'Status':<8}  {'Tamanho':>10}  {'Modificado':<22}  Extra"
    )
    print("-" * 90)

    # linha especial para CSVs anuais
    if csv_anos:
        print(
            f"{'CSVs anuais':<26}  {'ok':<8}  {len(csvs):>9}x  "
            f"{'—':<22}  anos: {csv_anos}"
        )
    else:
        print(f"{'CSVs anuais':<26}  {'ausente':<8}  {'—':>10}  —")

    for nome, path, contar in artefatos[1:]:  # pula CSVs anuais
        if not path.exists():
            print(f"{nome:<26}  {'ausente':<8}  {'—':>10}  —")
            continue
        extra = ""
        if contar:
            n = _count_csv_rows(path)
            extra = f"  {n} linhas"
        print(
            f"{nome:<26}  {'ok':<8}  {_fmt_size(path):>10}  {_fmt_mod(path):<22}{extra}"
        )

    print()
    return 0


# ===========================================================================
# Subcomando: limpar
# ===========================================================================


def cmd_limpar(args: argparse.Namespace) -> int:
    """Remove CSVs baixados (--tudo inclui geocache; requer --confirmar)."""
    csvs_anuais = sorted(DATA_DIR.glob("transacoes_imobiliarias_*.csv"))
    outros = [
        p
        for p in [DATA_DIR / "consolidado.csv", DATA_DIR / "consolidado_geo.csv"]
        if p.exists()
    ]
    alvos: list[Path] = csvs_anuais + outros

    if args.tudo:
        if not args.confirmar:
            print(
                "ATENÇÃO: --tudo remove o geocache.csv, que pode levar horas para\n"
                "         reconstruir. Adicione --confirmar para prosseguir.",
                file=sys.stderr,
            )
            return 1
        if GEOCACHE_CSV.exists():
            alvos.append(GEOCACHE_CSV)

    if not alvos:
        print("Nada para remover.")
        return 0

    print("Removendo:")
    for p in alvos:
        print(f"  {p}")
        p.unlink()

    print(f"\n{len(alvos)} arquivo(s) removido(s).")
    return 0


# ===========================================================================
# Parser argparse
# ===========================================================================


def _build_parser() -> argparse.ArgumentParser:
    """Constrói o parser principal com todos os subcomandos."""
    parser = argparse.ArgumentParser(
        prog="itbi",
        description="Pipeline ETL de dados ITBI — Niterói/RJ",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""\
Exemplos:
  itbi run                                       Pipeline completo (padrão)
  itbi run --anos 2023 2024                      Processa apenas 2023 e 2024
  itbi run --skip-download                       Reutiliza CSVs já baixados
  itbi run --skip-geo                            Reutiliza geocodificação existente
  itbi run --choropleth-geojson bairros.geojson  Adiciona choropleth por bairro
  itbi descobrir --json                          URLs em formato JSON
  itbi baixar --anos 2024 --force                Re-baixa só 2024
  itbi geocodificar --limite 10                  Geocodifica 10 endereços (teste)
  itbi mapa --choropleth-geojson bairros.geojson Regenera mapa com choropleth
  itbi status                                    Estado dos artefatos
  itbi limpar --tudo --confirmar                 Remove tudo incluindo geocache
""",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Exibe logs de depuração (DEBUG)"
    )

    sub = parser.add_subparsers(dest="comando", required=True, metavar="COMANDO")

    # ------------------------------------------------------------------ run
    p_run = sub.add_parser(
        "run",
        help="Executa o pipeline completo (padrão)",
        description="Pipeline completo: descobrir → baixar → consolidar → geocodificar → mapa.",
    )
    p_run.add_argument(
        "--anos",
        nargs="+",
        type=int,
        metavar="ANO",
        help="Restringe download a anos específicos (ex: --anos 2022 2023 2024)",
    )
    p_run.add_argument(
        "--skip-download",
        action="store_true",
        help="Assume que os CSVs já estão baixados em data/",
    )
    p_run.add_argument(
        "--skip-geo",
        action="store_true",
        help="Assume que consolidado_geo.csv já existe (pula geocodificação)",
    )
    p_run.add_argument(
        "--no-markers",
        action="store_true",
        help="Gera mapa sem marcadores clicáveis (mais leve para volumes grandes)",
    )
    p_run.add_argument(
        "--geocoder",
        type=str,
        default="nominatim",
        choices=["nominatim", "geocodebr", "auto"],
        help=(
            "Backend de geocodificação na etapa 4 (padrão: nominatim). "
            "Use geocodebr para motor local em R, ou auto para detectar."
        ),
    )
    p_run.add_argument(
        "--choropleth-geojson",
        dest="choropleth_geojson",
        default=None,
        metavar="GEOJSON",
        help=(
            "Caminho para GeoJSON local dos bairros (opcional). "
            "Quando fornecido, adiciona camada choropleth ao mapa."
        ),
    )
    p_run.add_argument(
        "--choropleth-key",
        dest="choropleth_key",
        type=str,
        default="nome",
        metavar="PROP",
        help="Propriedade GeoJSON para nomes de bairros (padrão: 'nome')",
    )

    # ------------------------------------------------------------ descobrir
    p_desc = sub.add_parser(
        "descobrir",
        help="Exibe as URLs dos CSVs na página da SMF Niterói",
        description="[ETAPA 1] Descobre URLs dos CSVs na página da SMF e imprime na tela.",
    )
    p_desc.add_argument(
        "--json",
        action="store_true",
        help="Saída em formato JSON em vez de tabela",
    )

    # --------------------------------------------------------------- baixar
    p_bx = sub.add_parser(
        "baixar",
        help="Faz download dos CSVs anuais",
        description="[ETAPA 2] Baixa CSVs anuais para data/itbi_niteroi/.",
    )
    p_bx.add_argument(
        "--anos",
        nargs="+",
        type=int,
        metavar="ANO",
        help="Anos a baixar (ex: --anos 2022 2023). Padrão: todos.",
    )
    p_bx.add_argument(
        "--force",
        action="store_true",
        help="Re-baixa mesmo que o arquivo já exista",
    )

    # ------------------------------------------------------------ consolidar
    sub.add_parser(
        "consolidar",
        help="Consolida os CSVs em consolidado.csv",
        description="[ETAPA 3] Lê todos os CSVs de data/ e gera consolidado.csv.",
    )

    # --------------------------------------------------------- geocodificar
    p_geo = sub.add_parser(
        "geocodificar",
        help="Geocodifica endereços via Nominatim",
        description=(
            "[ETAPA 4] Geocodifica endereços do consolidado.csv "
            "com fallback em 3 níveis (logradouro → bairro → centroide)."
        ),
    )
    p_geo.add_argument(
        "--reset-cache",
        action="store_true",
        help="Faz backup do geocache e reinicia geocodificação do zero",
    )
    p_geo.add_argument(
        "--limite",
        type=int,
        default=None,
        metavar="N",
        help="Geocodifica apenas N endereços novos (útil para testes)",
    )
    p_geo.add_argument(
        "--geocoder",
        type=str,
        default="nominatim",
        choices=["nominatim", "geocodebr", "auto"],
        help=(
            "Backend de geocodificação (padrão: nominatim). "
            "Use geocodebr para motor local em R, ou auto para detectar."
        ),
    )

    # ----------------------------------------------------------------- mapa
    p_mapa = sub.add_parser(
        "mapa",
        help="Gera heatmap HTML e JSON",
        description=(
            "[ETAPA 5] Gera docs/index.html (Folium) e docs/data/itbi_geo.json."
        ),
    )
    p_mapa.add_argument(
        "--no-markers",
        action="store_true",
        help="Omite marcadores clicáveis (mapa mais leve)",
    )
    p_mapa.add_argument(
        "--output",
        default=None,
        metavar="PATH",
        help=f"Caminho de saída do HTML (padrão: {OUTPUT_HTML})",
    )
    p_mapa.add_argument(
        "--choropleth-geojson",
        dest="choropleth_geojson",
        default=None,
        metavar="GEOJSON",
        help=(
            "Caminho para GeoJSON local dos bairros de Niterói (opcional). "
            "Quando fornecido, adiciona camada choropleth alternável via LayerControl."
        ),
    )
    p_mapa.add_argument(
        "--choropleth-key",
        dest="choropleth_key",
        type=str,
        default="nome",
        metavar="PROP",
        help=(
            "Propriedade GeoJSON usada para correlacionar nomes de bairros "
            "(padrão: 'nome'). Ex.: 'nome_bairro', 'NOME'."
        ),
    )

    # ------------------------------------------------------------- insights
    p_ins = sub.add_parser(
        "insights",
        help="Gera insights de valorização e joias escondidas",
        description=(
            "[ETAPA 6] Gera docs/data/itbi_insights.json com scores de "
            "valorização e joias escondidas baseados nos dados geocodificados."
        ),
    )
    p_ins.add_argument(
        "--input",
        default=None,
        metavar="CSV",
        help=f"CSV geocodificado de entrada (padrão: {DATA_DIR}/consolidado_geo.csv)",
    )
    p_ins.add_argument(
        "--output",
        default=None,
        metavar="JSON",
        help="Caminho de saída do JSON de insights (padrão: docs/data/itbi_insights.json)",
    )

    # ------------------------------------------------------------- backtest
    p_bt = sub.add_parser(
        "backtest",
        help="Executa mini-backtest de calibração de pesos/thresholds",
        description=(
            "[BACKTEST] Calibra pesos e thresholds dos scores via "
            "mini-backtest walk-forward e gera relatório."
        ),
    )
    p_bt.add_argument(
        "--input",
        default=None,
        metavar="CSV",
        help=f"CSV geocodificado de entrada (padrão: {DATA_DIR}/consolidado_geo.csv)",
    )

    # --------------------------------------------------------------- status
    sub.add_parser(
        "status",
        help="Exibe estado dos artefatos do pipeline",
        description="Inspeciona artefatos: existência, tamanho, data e número de linhas.",
    )

    # --------------------------------------------------------------- limpar
    p_lim = sub.add_parser(
        "limpar",
        help="Remove arquivos gerados pelo pipeline",
        description=(
            "Remove CSVs anuais e arquivos consolidados. "
            "--tudo inclui geocache (irreversível sem re-geocodificação)."
        ),
    )
    p_lim.add_argument(
        "--tudo",
        action="store_true",
        help="Remove também o geocache.csv (lento de reconstruir!)",
    )
    p_lim.add_argument(
        "--confirmar",
        action="store_true",
        help="Confirma remoção do geocache quando --tudo está ativo (obrigatório)",
    )

    # --- normalizar-enderecos ---
    p_normalizar = sub.add_parser(
        "normalizar-enderecos",
        help="Normaliza endereços via LLM (Fireworks AI) e salva JSON estruturado",
    )
    p_normalizar.add_argument(
        "--output",
        metavar="PATH",
        help="Caminho de saída do JSON (padrão: data/itbi_niteroi/enderecos_normalizados.json)",
    )
    p_normalizar.add_argument(
        "--batch-size",
        type=int,
        default=50,
        metavar="N",
        help="Endereços por chamada de API (padrão: 50)",
    )
    p_normalizar.add_argument(
        "--api-key",
        metavar="KEY",
        help="Fireworks AI API key (padrão: env FIREWORKS_API_KEY)",
    )

    # --- street-map ---
    p_sm = sub.add_parser(
        "street-map",
        help="Gera mapa de ruas coloridas por score de valorização",
    )
    p_sm.add_argument(
        "--input",
        metavar="CSV",
        help="CSV geocodificado (padrão: data/itbi_niteroi/consolidado_geo.csv)",
    )
    p_sm.add_argument(
        "--insights",
        metavar="JSON",
        help="JSON de insights (padrão: docs/data/itbi_insights.json)",
    )
    p_sm.add_argument(
        "--output",
        metavar="PATH",
        default="docs/street_map.html",
        help="Caminho de saída do HTML (padrão: docs/street_map.html)",
    )
    p_sm.add_argument(
        "--score",
        choices=["score_valorizacao", "score_joia_escondida"],
        default="score_valorizacao",
        help="Score a usar para colorir as ruas (padrão: score_valorizacao)",
    )
    p_sm.add_argument(
        "--janela",
        type=int,
        choices=[12, 24, 36],
        default=36,
        help="Janela temporal em meses (padrão: 36)",
    )

    return parser


# ===========================================================================
# Dispatch e entry point
# ===========================================================================

_HANDLER_MAP: dict[str, Callable[[argparse.Namespace], int]] = {
    "run": cmd_run,
    "descobrir": cmd_descobrir,
    "baixar": cmd_baixar,
    "consolidar": cmd_consolidar,
    "geocodificar": cmd_geocodificar,
    "mapa": cmd_mapa,
    "insights": cmd_insights,
    "backtest": cmd_backtest,
    "status": cmd_status,
    "limpar": cmd_limpar,
    "normalizar-enderecos": cmd_normalizar_enderecos,
    "street-map": cmd_street_map,
}


def main() -> None:
    """Entry point público — chamado por ``python -m itbi`` e pelo script ``itbi``."""
    parser = _build_parser()
    args = parser.parse_args()
    _setup_logging(verbose=args.verbose)

    handler = _HANDLER_MAP.get(args.comando)
    if handler is None:
        parser.print_help()
        sys.exit(1)

    sys.exit(handler(args))
