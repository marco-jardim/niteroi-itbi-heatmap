"""itbi/normalizacao_llm.py — Pré-normalização de endereços via LLM (Fireworks AI).

Uso:
    python -m itbi normalizar-enderecos [--batch-size 50] [--api-key KEY]

Saída: data/itbi_niteroi/enderecos_normalizados.json
  {
    "Av República 100, Centro, Niterói, RJ, Brasil": {
      "logradouro": "Avenida da República",
      "numero": "100",
      "complemento": "",
      "bairro": "Centro",
      "municipio": "Niterói",
      "estado": "RJ",
      "cep": ""
    },
    ...
  }

O arquivo é consumido por itbi/geocodificacao.py quando disponível.
"""

# ============================================================================
# Imports
# ============================================================================
import json
import logging
import os
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests

from itbi.config import DATA_DIR

# ============================================================================
# Constants
# ============================================================================
log = logging.getLogger(__name__)

FIREWORKS_API_URL = "https://api.fireworks.ai/inference/v1/chat/completions"
FIREWORKS_MODEL = "accounts/fireworks/models/kimi-k2p5"
ENDERECOS_NORM_JSON = DATA_DIR / "enderecos_normalizados.json"

_SYSTEM_PROMPT = """Você é um especialista em decomposição de endereços brasileiros.
Receberá uma lista de endereços brutos (um por linha), no formato típico de dados de ITBI:
  "<LOGRADOURO> <NUMERO?>, <BAIRRO>, <MUNICIPIO>, <ESTADO>, Brasil"

Sua tarefa é APENAS decompor cada endereço nos campos separados — NÃO normalize, expanda
abreviações nem corrija ortografia. Essas etapas são feitas por ferramenta downstream.

Retorne um objeto JSON onde cada chave é EXATAMENTE o endereço original (preserve espaços,
maiúsculas e pontuação sem qualquer alteração) e o valor é:
  logradouro  — nome do logradouro SEM número (ex: "Rua Das Flores" ou "Av. Ataíde")
  numero      — número predial se presente, caso contrário ""
  complemento — complemento se presente, caso contrário ""
  bairro      — bairro como aparece no endereço
  municipio   — município (padrão: "Niterói")
  estado      — UF (padrão: "RJ")
  cep         — CEP se presente, caso contrário ""

Regras críticas:
- A chave JSON deve ser IDÊNTICA à linha recebida (bit-a-bit).
- Se número estiver embutido no logradouro (ex: "Rua Foo 123"), separe: logradouro="Rua Foo", numero="123".
- Se não houver número, logradouro recebe todo o nome do logradouro.
- Retorne SOMENTE o JSON válido, sem markdown, sem explicações.

Exemplo de entrada:
Rua Tiradentes 250, Ingá, Niterói, RJ, Brasil
Trav. São João S/N, Barreto, Niterói, RJ, Brasil

Exemplo de saída:
{
  "Rua Tiradentes 250, Ingá, Niterói, RJ, Brasil": {
    "logradouro": "Rua Tiradentes",
    "numero": "250",
    "complemento": "",
    "bairro": "Ingá",
    "municipio": "Niterói",
    "estado": "RJ",
    "cep": ""
  },
  "Trav. São João S/N, Barreto, Niterói, RJ, Brasil": {
    "logradouro": "Trav. São João",
    "numero": "S/N",
    "complemento": "",
    "bairro": "Barreto",
    "municipio": "Niterói",
    "estado": "RJ",
    "cep": ""
  }
}"""

_CAMPO_PADRAO: dict[str, str] = {
    "logradouro": "",
    "numero": "",
    "complemento": "",
    "bairro": "",
    "municipio": "Niterói",
    "estado": "RJ",
    "cep": "",
}

# ============================================================================
# Core helpers
# ============================================================================


def _api_key(api_key: str | None = None) -> str:
    """Obtém a API key da Fireworks (parâmetro ou variável de ambiente)."""
    key = api_key or os.environ.get("FIREWORKS_API_KEY", "")
    if not key:
        raise ValueError(
            "API key da Fireworks não fornecida. "
            "Use --api-key ou exporte FIREWORKS_API_KEY."
        )
    return key


def _normalizar_batch(
    enderecos: list[str],
    api_key: str,
    tentativas: int = 3,
    pausa: float = 2.0,
) -> dict[str, dict[str, str]]:
    """Envia um batch de endereços ao Fireworks e retorna o dict normalizado."""
    prompt_usuario = "\n".join(enderecos)
    payload = {
        "model": FIREWORKS_MODEL,
        "messages": [
            {"role": "system", "content": _SYSTEM_PROMPT},
            {"role": "user", "content": prompt_usuario},
        ],
        "max_tokens": max(200 * len(enderecos), 2000),
        "temperature": 0.0,
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }

    for tentativa in range(1, tentativas + 1):
        try:
            resp = requests.post(
                FIREWORKS_API_URL,
                json=payload,
                headers=headers,
                timeout=120,
            )
            resp.raise_for_status()
            conteudo = resp.json()["choices"][0]["message"]["content"].strip()

            # Extrai bloco JSON da resposta (pode vir com ```json ... ```)
            if "```" in conteudo:
                conteudo = conteudo.split("```")[1]
                if conteudo.startswith("json"):
                    conteudo = conteudo[4:]
                conteudo = conteudo.strip()

            resultado: dict[str, Any] = json.loads(conteudo)
            # Garante que todos os campos existem e são strings
            saida: dict[str, dict[str, str]] = {}
            for end in enderecos:
                campos = resultado.get(end, {})
                saida[end] = {
                    campo: str(campos.get(campo, padrao))
                    for campo, padrao in _CAMPO_PADRAO.items()
                }
            return saida

        except (requests.RequestException, ValueError, KeyError, Exception) as exc:
            log.warning(
                "  Tentativa %d/%d — erro de rede: %s", tentativa, tentativas, exc
            )
        except (json.JSONDecodeError, KeyError, TypeError) as exc:
            log.warning(
                "  Tentativa %d/%d — resposta inválida: %s", tentativa, tentativas, exc
            )

        if tentativa < tentativas:
            time.sleep(pausa * tentativa)

    # Fallback: retorna dict vazio para todos do batch
    log.error(
        "  Batch falhou após %d tentativas. Endereços marcados como vazios.", tentativas
    )
    return {end: dict(_CAMPO_PADRAO) for end in enderecos}


# ============================================================================
# Public API
# ============================================================================


def normalizar_enderecos_llm(
    df: pd.DataFrame,
    output_path: Path = ENDERECOS_NORM_JSON,
    api_key: str | None = None,
    batch_size: int = 50,
    col_endereco: str = "ENDERECO",
) -> dict[str, dict[str, str]]:
    """Normaliza endereços únicos do DataFrame via Fireworks AI (Kimi K2.5).

    Endereços já presentes no arquivo de saída são reutilizados (cache incremental).

    Args:
        df: DataFrame com coluna de endereços brutos.
        output_path: Caminho do JSON de saída.
        api_key: API key da Fireworks (fallback: env FIREWORKS_API_KEY).
        batch_size: Número de endereços por chamada ao LLM.
        col_endereco: Nome da coluna de endereços no DataFrame.

    Returns:
        Dict {endereco_bruto: {campo: valor}}.
    """
    key = _api_key(api_key)

    if col_endereco not in df.columns:
        raise ValueError(f"Coluna '{col_endereco}' não encontrada no DataFrame.")

    enderecos_unicos: list[str] = df[col_endereco].dropna().unique().tolist()
    log.info("[NORM] %d endereços únicos encontrados.", len(enderecos_unicos))

    # Carrega cache existente
    cache: dict[str, dict[str, str]] = {}
    if output_path.exists():
        try:
            cache = json.loads(output_path.read_text(encoding="utf-8"))
            log.info("[NORM] Cache carregado: %d entradas.", len(cache))
        except (json.JSONDecodeError, OSError) as exc:
            log.warning("[NORM] Cache corrompido, iniciando do zero: %s", exc)

    pendentes = [e for e in enderecos_unicos if e not in cache]
    log.info("[NORM] %d endereços pendentes de normalização.", len(pendentes))

    # Processa em batches
    for inicio in range(0, len(pendentes), batch_size):
        lote = pendentes[inicio : inicio + batch_size]
        log.info(
            "[NORM] Batch %d/%d (%d endereços)...",
            inicio // batch_size + 1,
            -(-len(pendentes) // batch_size),
            len(lote),
        )
        resultado = _normalizar_batch(lote, key)
        cache.update(resultado)

        # Salva incrementalmente
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    log.info("[NORM] Normalização concluída. Salvo em: %s", output_path)
    return cache


def carregar_normalizados(
    path: Path = ENDERECOS_NORM_JSON,
) -> dict[str, dict[str, str]]:
    """Carrega o arquivo de endereços normalizados, se existir."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        log.warning("[NORM] Erro ao carregar normalizados: %s", exc)
        return {}


# ============================================================================
# Standalone
# ============================================================================

if __name__ == "__main__":
    import argparse

    from itbi.config import DATA_DIR as _DATA_DIR
    from itbi.consolidacao import carregar_e_consolidar

    def _build_arg_parser() -> argparse.ArgumentParser:
        p = argparse.ArgumentParser(
            description="Normaliza endereços ITBI via Fireworks AI (Kimi K2.5)."
        )
        p.add_argument(
            "--destino",
            type=Path,
            default=_DATA_DIR,
            help="Diretório com CSVs consolidados (padrão: data/itbi_niteroi)",
        )
        p.add_argument(
            "--output",
            type=Path,
            default=ENDERECOS_NORM_JSON,
            help="Caminho do JSON de saída",
        )
        p.add_argument("--batch-size", type=int, default=50, help="Endereços por batch")
        p.add_argument("--api-key", default=None, help="API key da Fireworks")
        return p

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    args = _build_arg_parser().parse_args()

    csvs = sorted(args.destino.glob("transacoes_imobiliarias_*.csv"))
    if not csvs:
        log.error(
            "Nenhum CSV anual encontrado em %s. Execute `itbi baixar` primeiro.",
            args.destino,
        )
        raise SystemExit(1)

    df_cons = carregar_e_consolidar(csvs)
    if "NOME DO LOGRADOURO" in df_cons.columns and "BAIRRO" in df_cons.columns:
        from itbi.geocodificacao import _montar_endereco

        df_cons["ENDERECO"] = df_cons.apply(_montar_endereco, axis=1)
    elif "ENDERECO" not in df_cons.columns:
        log.error("Coluna ENDERECO não encontrada. Verifique o consolidado.")
        raise SystemExit(1)

    normalizar_enderecos_llm(
        df_cons,
        output_path=args.output,
        api_key=args.api_key,
        batch_size=args.batch_size,
    )
