"""
Etapa 4 — Geocodificação de endereços via Nominatim (OpenStreetMap).

Estratégia de fallback em 3 níveis:

  - **Nível 1** (``"endereco"``): logradouro + bairro + Niterói, RJ, Brasil
  - **Nível 2** (``"bairro"``): bairro + Niterói, RJ, Brasil
  - **Nível 3** (``"centroide"``): centroide fixo do bairro (hardcoded para ~50
    bairros de Niterói — ver :data:`CENTROIDES_BAIRROS`)

Uso standalone::

    python -m itbi.geocodificacao
    python -m itbi.geocodificacao --reset-cache
    python -m itbi.geocodificacao --limite 20   # testa com 20 endereços
"""

import logging
import os
import re
import shutil
import subprocess
import tempfile
import unicodedata
from pathlib import Path

import pandas as pd
from geopy.exc import GeocoderServiceError, GeocoderTimedOut, GeocoderUnavailable
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from tqdm import tqdm

from itbi.config import (
    DATA_DIR,
    GEOCACHE_CSV,
    NOMINATIM_USER_AGENT,
    NOMINATIM_DELAY,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Tipo auxiliar: resultado de geocodificação
# (lat, lon, nivel) onde nivel ∈ {"endereco","bairro","centroide","nenhum"}
# ---------------------------------------------------------------------------
GeoEntry = tuple[float | None, float | None, str]

GEOCODER_OPCOES = ("nominatim", "geocodebr", "auto")

# ===========================================================================
# Centroides fixos dos bairros de Niterói — fallback nível 3
#
# Fonte: estimativas baseadas em OpenStreetMap / dados da Prefeitura de Niterói.
# Cobertura: ~50 bairros reconhecidos pelo IBGE/Prefeitura.
# Tolerância: ±300 m aceitável para heat-map por logradouro.
# ===========================================================================

CENTROIDES_BAIRROS: dict[str, tuple[float, float]] = {
    # Zona Sul / Orla
    "Icaraí": (-22.9043, -43.1199),
    "São Francisco": (-22.9307, -43.1229),
    "Charitas": (-22.9471, -43.1282),
    "Jurujuba": (-22.9458, -43.1202),
    "Boa Viagem": (-22.9179, -43.1165),
    "Gragoatá": (-22.8970, -43.1281),
    "Ponta D'Areia": (-22.9024, -43.1254),
    "Preventório": (-22.9371, -43.1258),
    "Maceió": (-22.9310, -43.1065),
    "Sapê": (-22.9188, -43.0859),
    # Área Central
    "Centro": (-22.8971, -43.1152),
    "Ingá": (-22.9031, -43.1168),
    "São Domingos": (-22.9103, -43.1069),
    "Vital Brazil": (-22.9174, -43.1062),
    "Largo Da Batalha": (-22.9021, -43.1053),
    "Santa Rosa": (-22.9121, -43.0998),
    "Jardim Icaraí": (-22.9107, -43.1209),
    "Morro Do Estado": (-22.8958, -43.1131),
    "Ilha Da Conceição": (-22.8905, -43.1157),
    "Jacaré": (-22.9082, -43.1168),
    # Zona Norte / Interior
    "Fonseca": (-22.8808, -43.0828),
    "Barreto": (-22.8669, -43.0914),
    "Santana": (-22.8905, -43.1001),
    "Pé Pequeno": (-22.8931, -43.0991),
    "Cubango": (-22.8853, -43.0918),
    "Caramujo": (-22.8850, -43.0811),
    "Tenente Jardim": (-22.8809, -43.0985),
    "Cantagalo": (-22.8760, -43.0968),
    "Neves": (-22.8629, -43.0965),
    "Mutondo": (-22.8673, -43.0884),
    "Serra Grande": (-22.8643, -43.0817),
    "Palmeira": (-22.8700, -43.0884),
    "Baldeador": (-22.8556, -43.0845),
    "Maria Paula": (-22.8565, -43.0799),
    "Colubandê": (-22.8605, -43.0744),
    "Rio Vermelho": (-22.8543, -43.0704),
    "Rio Do Ouro": (-22.8640, -43.0660),
    "Cafubá": (-22.8734, -43.0481),
    "Pendotiba": (-22.8871, -43.0486),
    "Várzea Das Moças": (-22.8709, -43.1093),
    "Divina Providência": (-22.8744, -43.0866),
    "Niterolandia": (-22.8612, -43.0649),
    "Matapaca": (-22.8481, -43.1007),
    "Sampaio": (-22.9068, -43.0820),
    # Região Oceânica
    "Piratininga": (-22.9485, -43.0697),
    "Itaipu": (-22.9523, -43.0611),
    "Itacoatiara": (-22.9611, -43.0543),
    "Camboinhas": (-22.9647, -43.0622),
    "Maravista": (-22.9405, -43.0705),
    "Engenho Do Mato": (-22.9354, -43.0605),
}


ABREVIACOES_LOGRADOURO: tuple[tuple[str, str], ...] = (
    (r"\bav\.?\b", "avenida"),
    (r"\br\.?\b", "rua"),
    (r"\btrav\.?\b", "travessa"),
    (r"\brod\.?\b", "rodovia"),
    (r"\bestr\.?\b", "estrada"),
    (r"\bal\.?\b", "alameda"),
    (r"\bpca\.?\b", "praca"),
)


# ===========================================================================
# Helpers de montagem de endereço
# ===========================================================================


def _montar_endereco(row: pd.Series) -> str:
    """Monta string de endereço completo para geocodificação (nível 1).

    Formato: ``"<logradouro>, <bairro>, Niterói, RJ, Brasil"``

    Args:
        row: Linha do DataFrame com chaves ``NOME DO LOGRADOURO`` e ``BAIRRO``.

    Returns:
        String formatada para envio ao Nominatim.
    """
    logradouro = _texto_limpo(row.get("NOME DO LOGRADOURO", ""))
    bairro = _texto_limpo(row.get("BAIRRO", ""))
    return f"{logradouro}, {bairro}, Niterói, RJ, Brasil"


def _montar_endereco_bairro(bairro: str) -> str:
    """Monta string de bairro para geocodificação de fallback (nível 2).

    Args:
        bairro: Nome do bairro (já com title-case).

    Returns:
        String ``"<bairro>, Niterói, RJ, Brasil"``.
    """
    return f"{bairro}, Niterói, RJ, Brasil"


def _montar_endereco_sem_bairro(logradouro: str) -> str:
    """Monta string de endereço sem bairro para segunda tentativa de nível 1."""
    return f"{logradouro}, Niterói, RJ, Brasil"


def _texto_limpo(valor: object) -> str:
    """Normaliza texto de entrada removendo nulos/NaN e espaços extras."""
    if valor is None:
        return ""
    try:
        if bool(pd.isna(valor)):
            return ""
    except (TypeError, ValueError):
        pass
    if isinstance(valor, (list, tuple, dict, set)):
        return ""
    texto = str(valor).strip()
    if texto.lower() == "nan":
        return ""
    return re.sub(r"\s+", " ", texto)


def _normalizar_logradouro(logradouro: str) -> str:
    """Normaliza abreviações comuns para melhorar match no Nominatim."""
    if not logradouro:
        return ""
    texto = _remover_acentos(logradouro).lower()
    texto = re.sub(r"[\.;:_\-]+", " ", texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    for padrao, substituto in ABREVIACOES_LOGRADOURO:
        texto = re.sub(padrao, substituto, texto)
    texto = re.sub(r"\s+", " ", texto).strip()
    return texto.title()


def _deve_tentar_retry_sem_bairro(original: str, normalizado: str) -> bool:
    """Decide se vale tentar geocodificação nível 1 sem bairro.

    A tentativa extra é feita apenas quando a normalização altera o texto,
    reduzindo chamadas adicionais desnecessárias ao geocodificador.
    """
    if not original or not normalizado:
        return False
    return original.strip() != normalizado.strip()


def _remover_acentos(texto: str) -> str:
    """Remove acentuação preservando caracteres ASCII básicos."""
    return "".join(
        c for c in unicodedata.normalize("NFKD", texto) if not unicodedata.combining(c)
    )


def _centroide_bairro(bairro: str) -> tuple[float, float] | None:
    """Retorna o centroide fixo do bairro (nível 3) ou ``None`` se não mapeado.

    Tenta correspondência exata primeiro; cai em comparação case-insensitive.

    Args:
        bairro: Nome do bairro (qualquer capitalização).

    Returns:
        Tupla ``(lat, lon)`` ou ``None`` se o bairro não estiver em
        :data:`CENTROIDES_BAIRROS`.
    """
    if bairro in CENTROIDES_BAIRROS:
        return CENTROIDES_BAIRROS[bairro]
    bairro_lower = bairro.lower()
    for nome, coords in CENTROIDES_BAIRROS.items():
        if nome.lower() == bairro_lower:
            return coords
    return None


def _normalizar_geocoder(geocoder: str) -> str:
    valor = geocoder.strip().lower()
    if valor not in GEOCODER_OPCOES:
        raise ValueError(
            f"Geocoder inválido: '{geocoder}'. Opções: {', '.join(GEOCODER_OPCOES)}"
        )
    return valor


def _rscript_disponivel() -> bool:
    """Retorna True quando o executável Rscript está disponível no PATH."""
    try:
        subprocess.run(
            ["Rscript", "--version"],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=10,
        )
    except (FileNotFoundError, OSError, subprocess.SubprocessError):
        return False
    return True


def _geocodebr_disponivel() -> bool:
    """Retorna True quando o pacote R geocodebr está instalado."""
    if not _rscript_disponivel():
        return False
    cmd = [
        "Rscript",
        "-e",
        "quit(status=ifelse(requireNamespace('geocodebr', quietly=TRUE),0,1))",
    ]
    try:
        proc = subprocess.run(
            cmd,
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=20,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return proc.returncode == 0


def _mapear_nivel_precisao_geocodebr(precisao: str) -> str:
    """Converte precisão textual do geocodebr para NIVEL_GEO interno."""
    txt = _remover_acentos(_texto_limpo(precisao)).lower()
    if any(chave in txt for chave in ("porta", "numero", "logradouro", "endereco")):
        return "endereco"
    if any(chave in txt for chave in ("bairro", "setor", "localidade")):
        return "bairro"
    return "endereco"


def _quebrar_endereco(
    endereco: str,
    normalizados: dict[str, dict[str, str]] | None = None,
) -> tuple[str, str, str, str]:
    """Converte string única em campos para o geocodebr.

    Se ``normalizados`` for fornecido e contiver o endereço, usa os campos
    já estruturados (logradouro, numero, bairro, municipio, estado).
    """
    if normalizados and endereco in normalizados:
        n = normalizados[endereco]
        logradouro = n.get("logradouro", "").strip()
        numero = n.get("numero", "").strip()
        bairro = n.get("bairro", "").strip()
        municipio = n.get("municipio", "Niterói").strip() or "Niterói"
        estado = n.get("estado", "RJ").strip() or "RJ"
        if numero:
            logradouro = f"{logradouro}, {numero}" if logradouro else numero
        return (logradouro, bairro, municipio, estado)

    partes = [p.strip() for p in endereco.split(",")]
    logradouro = partes[0] if partes else ""
    bairro = partes[1] if len(partes) > 1 else ""
    return (logradouro, bairro, "Niterói", "RJ")


def _geocodificar_lote_geocodebr(
    enderecos: list[str],
    normalizados: dict[str, dict[str, str]] | None = None,
) -> dict[str, GeoEntry]:
    """Geocodifica endereços em lote usando geocodebr via Rscript."""
    if not enderecos:
        return {}

    script = r"""
args <- commandArgs(trailingOnly = TRUE)
in_csv <- args[[1]]
out_csv <- args[[2]]

suppressPackageStartupMessages(library(geocodebr))
suppressPackageStartupMessages(library(enderecobr))

df <- read.csv(in_csv, stringsAsFactors = FALSE, fileEncoding = "UTF-8")

# Padroniza com enderecobr (gera colunas *_padr)
campos_pad <- enderecobr::correspondencia_campos(
  logradouro = "logradouro",
  municipio  = "municipio",
  estado     = "estado"
)
df_pad <- enderecobr::padronizar_enderecos(
  df,
  campos_do_endereco = campos_pad,
  formato_estados    = "sigla",
  formato_numeros    = "integer"
)

# Preenche colunas _padr ausentes (geocodebr exige todas as 6)
# numero_padr deve ser inteiro (NA_integer_) para evitar erro de tipo no DuckDB
for (col in c("bairro_padr", "cep_padr")) {
  if (!col %in% names(df_pad)) df_pad[[col]] <- NA_character_
}
if (!"numero_padr" %in% names(df_pad)) df_pad[["numero_padr"]] <- NA_integer_

campos <- geocodebr::definir_campos(
  logradouro = "logradouro",
  localidade = "localidade",
  municipio  = "municipio",
  estado     = "estado"
)

# resultado_completo = FALSE evita bug de v0.5.0+ onde a coluna "empate"
# e consultada em output_db mas nunca inserida (geocodebr issue #XXX).
# lat/lon/precisao sao retornados independente desse parametro.
res <- geocodebr::geocode(
  enderecos          = df_pad,
  campos_endereco    = campos,
  padronizar_enderecos = FALSE,
  resultado_completo = FALSE,
  resolver_empates   = TRUE,
  resultado_sf       = FALSE,
  verboso            = FALSE
)

nm <- names(res)
lat_idx  <- which(grepl("^lat|latitude",          nm, ignore.case = TRUE))[1]
lon_idx  <- which(grepl("^lon|lng|long|longitude", nm, ignore.case = TRUE))[1]
prec_idx <- which(grepl("precis",                  nm, ignore.case = TRUE))[1]

lat  <- if (!is.na(lat_idx))  suppressWarnings(as.numeric(res[[lat_idx]]))  else rep(NA_real_, nrow(df_pad))
lon  <- if (!is.na(lon_idx))  suppressWarnings(as.numeric(res[[lon_idx]]))  else rep(NA_real_, nrow(df_pad))
prec <- if (!is.na(prec_idx)) as.character(res[[prec_idx]])                  else rep("", nrow(df_pad))

out <- data.frame(
  ENDERECO = df_pad$ENDERECO,
  LAT      = lat,
  LON      = lon,
  PRECISAO = prec,
  stringsAsFactors = FALSE
)
write.csv(out, out_csv, row.names = FALSE, fileEncoding = "UTF-8")
"""

    resultados: dict[str, GeoEntry] = {}
    with tempfile.TemporaryDirectory(prefix="itbi_geocodebr_") as tmpdir:
        in_csv = Path(tmpdir) / "input.csv"
        out_csv = Path(tmpdir) / "output.csv"

        rows = []
        for endereco in enderecos:
            logradouro, bairro, municipio, estado = _quebrar_endereco(
                endereco, normalizados=normalizados
            )
            rows.append(
                {
                    "ENDERECO": endereco,
                    "logradouro": logradouro,
                    "localidade": bairro,
                    "municipio": municipio,
                    "estado": estado,
                }
            )

        pd.DataFrame(rows).to_csv(in_csv, index=False, encoding="utf-8")

        try:
            proc = subprocess.run(
                ["Rscript", "-e", script, str(in_csv), str(out_csv)],
                check=False,
                capture_output=True,
                text=True,
                timeout=300,
                env={**os.environ, "LANG": "C.UTF-8"},
            )
        except (OSError, subprocess.SubprocessError) as exc:
            raise RuntimeError(f"Falha ao executar Rscript/geocodebr: {exc}") from exc

        if proc.returncode != 0:
            erro = (proc.stderr or proc.stdout or "erro desconhecido").strip()
            raise RuntimeError(f"geocodebr falhou: {erro}")

        if not out_csv.exists():
            raise RuntimeError("geocodebr não gerou arquivo de saída")

        df_out = pd.read_csv(out_csv)
        for rec in df_out.to_dict(orient="records"):
            endereco = _texto_limpo(rec.get("ENDERECO", ""))
            if not endereco:
                continue

            raw_lat = rec.get("LAT")
            raw_lon = rec.get("LON")
            if raw_lat is None or raw_lon is None:
                resultados[endereco] = (None, None, "nenhum")
                continue

            try:
                lat = float(raw_lat)
                lon = float(raw_lon)
            except (TypeError, ValueError):
                resultados[endereco] = (None, None, "nenhum")
                continue

            precisao = _texto_limpo(rec.get("PRECISAO", ""))
            nivel = _mapear_nivel_precisao_geocodebr(precisao)
            resultados[endereco] = (lat, lon, nivel)

    return resultados


# ===========================================================================
# Etapa 4 — Geocodificação
# ===========================================================================


def geocodificar(
    df: pd.DataFrame,
    cache_path: Path = GEOCACHE_CSV,
    reset_cache: bool = False,
    limite: int | None = None,
    geocoder: str = "nominatim",
) -> pd.DataFrame:
    """Geocodifica endereços únicos via Nominatim com fallback em 3 níveis.

    Nível 1 — endereço completo (logradouro + bairro + cidade).
    Nível 2 — bairro + cidade (quando o endereço completo não é resolvido).
    Nível 3 — centroide fixo do bairro (quando até o bairro falha).

    O resultado de cada nível é registrado na coluna ``NIVEL_GEO``
    (``"endereco"`` | ``"bairro"`` | ``"centroide"``), que pode ser usada
    como fator de confiança na análise.

    Cache append-only: resultados novos são *adicionados* ao ``cache_path``
    sem sobrescrever entradas existentes.  ``reset_cache=True`` faz backup
    automático antes de apagar o cache.

    Args:
        df:          DataFrame com colunas ``NOME DO LOGRADOURO`` e ``BAIRRO``.
        cache_path:  Caminho do CSV de cache geocodificação.
        reset_cache: Se ``True``, faz backup do cache existente e começa do zero.
        limite:      Máximo de endereços *novos* a geocodificar nesta execução.
                      ``None`` = sem limite (comportamento padrão).
        geocoder:    Backend de geocodificação:
                     ``"nominatim"`` | ``"geocodebr"`` | ``"auto"``.

    Returns:
        DataFrame com colunas ``LAT``, ``LON`` e ``NIVEL_GEO`` adicionadas,
        filtrado para manter apenas as linhas com coordenadas válidas.
    """
    log.info("[ETAPA 4] Geocodificando endereços...")

    geocoder_normalizado = _normalizar_geocoder(geocoder)
    geocoder_escolhido = geocoder_normalizado
    if geocoder_normalizado in ("geocodebr", "auto"):
        if _geocodebr_disponivel():
            geocoder_escolhido = "geocodebr"
        elif geocoder_normalizado == "geocodebr":
            log.warning(
                "geocodebr solicitado, mas indisponível. Usando Nominatim como fallback."
            )
            geocoder_escolhido = "nominatim"
        else:
            geocoder_escolhido = "nominatim"

    log.info("  Geocoder selecionado: %s", geocoder_escolhido)

    geocode = None
    if geocoder_escolhido == "nominatim":
        geolocator = Nominatim(user_agent=NOMINATIM_USER_AGENT)
        geocode = RateLimiter(
            geolocator.geocode,
            min_delay_seconds=NOMINATIM_DELAY,
            error_wait_seconds=5,
        )

    # -----------------------------------------------------------------------
    # Lê / reseta cache
    # -----------------------------------------------------------------------
    cache: dict[str, GeoEntry] = {}

    if cache_path.exists() and not reset_cache:
        try:
            df_cache = pd.read_csv(cache_path)
            # Retrocompatibilidade: caches antigos sem coluna NIVEL_GEO
            if "NIVEL_GEO" not in df_cache.columns:
                df_cache["NIVEL_GEO"] = "endereco"
            for rec in df_cache.to_dict(orient="records"):
                try:
                    endereco = str(rec.get("ENDERECO", "")).strip()
                    raw_lat = rec.get("LAT")
                    raw_lon = rec.get("LON")
                    if raw_lat is None or raw_lon is None:
                        continue
                    lat = float(raw_lat)
                    lon = float(raw_lon)
                    nivel = str(rec.get("NIVEL_GEO", "endereco")).strip() or "endereco"
                    if endereco:
                        cache[endereco] = (lat, lon, nivel)
                except (ValueError, TypeError):
                    pass  # ignora entradas corrompidas
            log.info("  Cache: %d entradas carregadas", len(cache))
        except (pd.errors.ParserError, OSError, KeyError, ValueError) as exc:
            log.warning("  Erro ao ler cache (%s). Iniciando cache vazio.", exc)

    elif cache_path.exists() and reset_cache:
        backup = cache_path.with_suffix(".backup.csv")
        shutil.copy2(cache_path, backup)
        cache_path.unlink()
        log.warning("  reset_cache=True: backup salvo em '%s', cache zerado.", backup)

    # -----------------------------------------------------------------------
    # Monta endereços e identifica bairros por endereço (para fallback)
    # -----------------------------------------------------------------------
    df = df.copy()
    df["ENDERECO"] = df.apply(_montar_endereco, axis=1)

    # Mapa endereco → bairro para usar no fallback sem parsear a string
    bairro_col = (
        df["BAIRRO"] if "BAIRRO" in df.columns else pd.Series("", index=df.index)
    )
    endereco_bairro: dict[str, str] = {
        endereco: _texto_limpo(bairro)
        for endereco, bairro in zip(df["ENDERECO"], bairro_col.fillna(""))
    }

    enderecos_novos = [e for e in df["ENDERECO"].unique() if e not in cache]
    if limite is not None:
        enderecos_novos = enderecos_novos[:limite]
        log.info("  Limite aplicado: geocodificando até %d endereço(s) novo(s)", limite)

    log.info("  %d endereço(s) novo(s) para geocodificar", len(enderecos_novos))

    # -----------------------------------------------------------------------
    # Loop de geocodificação com fallback em 3 níveis
    # -----------------------------------------------------------------------
    novos: dict[str, GeoEntry] = {}

    if geocoder_escolhido == "geocodebr":
        try:
            # Carrega mapa de normalização LLM se disponível
            normalizados: dict[str, dict[str, str]] | None = None
            try:
                from itbi.normalizacao_llm import (
                    carregar_normalizados,
                    ENDERECOS_NORM_JSON,
                )

                _path_norm = ENDERECOS_NORM_JSON
                if _path_norm.exists():
                    normalizados = carregar_normalizados(_path_norm)
                    log.info(
                        "  [normalização] JSON carregado: %d endereços estruturados",
                        len(normalizados),
                    )
            except ImportError:
                pass

            lote = _geocodificar_lote_geocodebr(
                enderecos_novos, normalizados=normalizados
            )
        except RuntimeError as exc:
            log.warning("  geocodebr falhou (%s). Recuando para Nominatim.", exc)
            geocoder_escolhido = "nominatim"
            geolocator = Nominatim(user_agent=NOMINATIM_USER_AGENT)
            geocode = RateLimiter(
                geolocator.geocode,
                min_delay_seconds=NOMINATIM_DELAY,
                error_wait_seconds=5,
            )
        else:
            for endereco in enderecos_novos:
                bairro = endereco_bairro.get(endereco, "")
                entry = lote.get(endereco, (None, None, "nenhum"))
                if entry[0] is None:
                    centroide = _centroide_bairro(bairro)
                    if centroide:
                        entry = (centroide[0], centroide[1], "centroide")
                        log.info(
                            "  Fallback nível 3 (centroide): '%s' → bairro '%s'",
                            endereco,
                            bairro,
                        )
                    else:
                        log.warning("  Não geocodificado: '%s'", endereco)
                novos[endereco] = entry

    if geocoder_escolhido == "nominatim":
        if geocode is None:
            raise ValueError("Geocoder Nominatim não inicializado")

        for endereco in tqdm(enderecos_novos, desc="Geocodificando", unit="end"):
            bairro = endereco_bairro.get(endereco, "")
            entry: GeoEntry = (None, None, "nenhum")

            try:
                # — Nível 1: endereço completo (com segunda tentativa sem bairro) —
                loc = geocode(endereco)
                if loc:
                    entry = (loc.latitude, loc.longitude, "endereco")

                else:
                    logradouro = endereco.split(",", maxsplit=1)[0].strip()
                    logradouro_norm = _normalizar_logradouro(logradouro)
                    loc1b = None
                    if _deve_tentar_retry_sem_bairro(logradouro, logradouro_norm):
                        end_sem_bairro = _montar_endereco_sem_bairro(logradouro_norm)
                        loc1b = geocode(end_sem_bairro)
                        if loc1b:
                            entry = (loc1b.latitude, loc1b.longitude, "endereco")
                            log.info(
                                "  Retry nível 1 (sem bairro): '%s' → '%s'",
                                endereco,
                                end_sem_bairro,
                            )

                    if not loc1b:
                        # — Nível 2: bairro + cidade —
                        end_bairro = _montar_endereco_bairro(bairro)
                        loc2 = geocode(end_bairro) if bairro else None
                        if loc2:
                            entry = (loc2.latitude, loc2.longitude, "bairro")
                            log.info(
                                "  Fallback nível 2 (bairro): '%s' → '%s'",
                                endereco,
                                end_bairro,
                            )
                        else:
                            # — Nível 3: centroide fixo —
                            centroide = _centroide_bairro(bairro)
                            if centroide:
                                entry = (centroide[0], centroide[1], "centroide")
                                log.info(
                                    "  Fallback nível 3 (centroide): '%s' → bairro '%s'",
                                    endereco,
                                    bairro,
                                )
                            else:
                                log.warning("  Não geocodificado: '%s'", endereco)

            except (GeocoderServiceError, GeocoderTimedOut, GeocoderUnavailable) as exc:
                log.warning("  Falha em '%s': %s", endereco, exc)
                # Tenta centroide mesmo após exceção para não perder o ponto
                centroide = _centroide_bairro(bairro)
                if centroide:
                    entry = (centroide[0], centroide[1], "centroide")
                    log.info("  Fallback nível 3 pós-exceção: bairro '%s'", bairro)

            novos[endereco] = entry

    # -----------------------------------------------------------------------
    # Persiste novas entradas no cache (append-only; só entradas com coords)
    # -----------------------------------------------------------------------
    cache.update(novos)

    novos_validos = {k: v for k, v in novos.items() if v[0] is not None}
    if novos_validos:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        header = not cache_path.exists()
        mode = "w" if header else "a"
        pd.DataFrame(
            [(k, v[0], v[1], v[2]) for k, v in novos_validos.items()],
            columns=["ENDERECO", "LAT", "LON", "NIVEL_GEO"],
        ).to_csv(
            cache_path, mode=mode, header=header, index=False, encoding="utf-8-sig"
        )
        log.info("  %d novo(s) endereço(s) salvo(s) no cache", len(novos_validos))

    # -----------------------------------------------------------------------
    # Mapeia coordenadas e nível de volta ao DataFrame
    # -----------------------------------------------------------------------
    df["LAT"] = df["ENDERECO"].map(lambda e: cache.get(e, (None, None, ""))[0])
    df["LON"] = df["ENDERECO"].map(lambda e: cache.get(e, (None, None, ""))[1])
    df["NIVEL_GEO"] = df["ENDERECO"].map(
        lambda e: cache.get(e, (None, None, "desconhecido"))[2]
    )

    n_ok = df["LAT"].notna().sum()
    log.info("  Geocodificados com sucesso: %d/%d", n_ok, len(df))

    distribuicao = (
        df["NIVEL_GEO"].fillna("desconhecido").value_counts(dropna=False).to_dict()
    )
    log.info("  Distribuição NIVEL_GEO: %s", distribuicao)

    return df.dropna(subset=["LAT", "LON"])


# ===========================================================================
# Entrypoint standalone: python -m itbi.geocodificacao
# ===========================================================================


def _build_arg_parser():  # type: ignore[return]
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m itbi.geocodificacao",
        description=(
            "[ETAPA 4] Geocodifica endereços do consolidado.csv "
            "e gera consolidado_geo.csv."
        ),
    )
    parser.add_argument(
        "--reset-cache",
        action="store_true",
        help="Faz backup do cache existente e inicia geocodificação do zero.",
    )
    parser.add_argument(
        "--limite",
        type=int,
        default=None,
        metavar="N",
        help="Geocodifica apenas os N primeiros endereços novos (útil para testes).",
    )
    parser.add_argument(
        "--geocoder",
        type=str,
        default="nominatim",
        choices=GEOCODER_OPCOES,
        help=(
            "Backend de geocodificação (padrão: nominatim). "
            "Use 'geocodebr' para motor local em R, ou 'auto' para detectar."
        ),
    )
    parser.add_argument(
        "--destino",
        type=Path,
        default=DATA_DIR,
        metavar="DIR",
        help=f"Diretório com consolidado.csv e destino do consolidado_geo.csv "
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

    csv_path = destino / "consolidado.csv"
    if not csv_path.exists():
        log.error(
            "Arquivo não encontrado: '%s'. Execute 'python -m itbi.consolidacao' primeiro.",
            csv_path,
        )
        sys.exit(1)

    df = pd.read_csv(csv_path)
    df_geo = geocodificar(
        df,
        cache_path=destino / "geocache.csv",
        reset_cache=args.reset_cache,
        limite=args.limite,
        geocoder=args.geocoder,
    )

    saida = destino / "consolidado_geo.csv"
    df_geo.to_csv(saida, index=False, encoding="utf-8-sig")
    print(f"\nGeocodificado salvo: {saida} ({len(df_geo)} linhas)")
    sys.exit(0)
