# PLAN — niteroi-itbi-heatmap

> Mapa de calor dos valores de imóveis transacionados em Niterói, com base nos dados públicos de ITBI da Secretaria Municipal de Fazenda.

---

## 1. Visão geral

### Objetivo
Coletar, geocodificar e visualizar as médias de valores de transações imobiliárias (ITBI) por logradouro em Niterói/RJ, publicadas como dados abertos pela Prefeitura.

### Arquitetura de publicação

```
[Local] CLI itbi → scripts individuais por etapa
                         ↓
         data/itbi_niteroi/  (CSV bruto + geocache — não commitado)
                         ↓
         docs/index.html + docs/data/itbi_geo.json
                         ↓
                 GitHub Pages (público, zero custo)
```

**Por que essa arquitetura?**
- Processamento pesado (geocodificação ~minutosa) roda localmente
- GitHub Pages serve só HTML estático + JSON pré-processado → zero custo, zero backend
- Scripts separados por etapa permitem re-executar etapas individualmente sem rodar o pipeline todo
- CLI unificado (`python -m itbi` ou `itbi`) orquestra as etapas com flags claras

---

## 2. Estado atual dos dados

| Atributo | Detalhe |
|----------|---------|
| **Fonte** | SMF Niterói — https://www.fazenda.niteroi.rj.gov.br/site/dados-das-transacoes-imobiliarias/ |
| **Formato** | 5 CSVs anuais (2020–2024), WordPress estático, download direto |
| **Granularidade** | Médias por **logradouro** (não por transação individual) |
| **Campos** | 11 colunas: bairro, logradouro, 3× área, 2× valor, qtd transações, tipologia, natureza, ano |
| **Limitação** | Dados agregados — o mapa mostra "rua mais cara", não "imóvel mais caro" |

---

## 3. Arquitetura de scripts refatorada

O monólito `scripts/scraper.py` (450 linhas) deve ser decomposto em módulos independentes com interface clara. Cada módulo é executável standalone E orquestrável pelo CLI.

### Estrutura alvo

```
niteroi-itbi-heatmap/
├── itbi/                          # Pacote Python instalável
│   ├── __init__.py
│   ├── __main__.py                # Ponto de entrada: python -m itbi
│   ├── cli.py                     # argparse CLI com subcomandos
│   ├── config.py                  # Constantes e caminhos centralizados
│   ├── descoberta.py              # Etapa 1: descoberta dinâmica de URLs
│   ├── download.py                # Etapa 2: download de CSVs com cache
│   ├── consolidacao.py            # Etapa 3: limpeza e consolidação
│   ├── geocodificacao.py          # Etapa 4: geocodificação Nominatim
│   └── heatmap.py                 # Etapa 5: geração do mapa Folium
├── scripts/
│   └── scraper.py                 # Legado — substituído pelo pacote itbi/
├── docs/                          # GitHub Pages root (commitado)
│   ├── index.html
│   └── data/
│       └── itbi_geo.json
├── data/                          # Cache local (NÃO commitado)
│   └── itbi_niteroi/
│       ├── transacoes_imobiliarias_YYYY.csv
│       ├── consolidado.csv
│       ├── consolidado_geo.csv
│       └── geocache.csv
├── tests/
│   ├── test_descoberta.py
│   ├── test_consolidacao.py
│   └── test_geocodificacao.py
├── pyproject.toml                 # Torna `itbi` instalável e CLI disponível
├── AGENTS.md
├── PLAN.md
├── README.md
└── requirements.txt
```

### Interface CLI (subcomandos)

```bash
# Pipeline completo (padrão)
itbi run

# Etapas individuais
itbi descobrir                  # Imprime URLs encontradas
itbi baixar [--anos 2022 2023]  # Baixa CSVs seletivamente
itbi consolidar                 # Lê CSVs e gera consolidado.csv
itbi geocodificar [--reset-cache]  # Geocodifica endereços
itbi mapa [--no-markers]        # Gera index.html e itbi_geo.json

# Utilitários
itbi status                     # Mostra estado dos artefatos (existem? tamanho? data?)
itbi limpar [--tudo]            # Remove CSVs baixados (--tudo inclui geocache)
```

---

## 4. Task list de implementação

### Fase 0 — Scaffolding do pacote (pré-requisito de tudo)

- [x] [tier:medium] **0.1** Criar `itbi/__init__.py` com `__version__ = "0.1.0"`
- [x] [tier:medium] **0.2** Criar `itbi/config.py` — extrair TODAS as constantes de `scraper.py`:
  - `BASE_URL`, `DATA_DIR`, `DOCS_DIR`, `OUTPUT_HTML`, `DATA_JSON`, `GEOCACHE_CSV`
  - `CSV_URLS_FALLBACK` (dict de fallback)
  - `HEADERS` (User-Agent HTTP)
  - `NOMINATIM_USER_AGENT`, `NOMINATIM_DELAY` (1.1s)
- [x] [tier:medium] **0.3** Criar `pyproject.toml` com entry point `itbi = itbi.__main__:main`
- [x] [tier:medium] **0.4** Criar `itbi/__main__.py` que chama `itbi.cli.main()`

**Critério de aceite (DoD) da Fase 0**
- Pacote `itbi` importável em ambiente limpo
- Comando `python -m itbi --help` executa sem erro
- Constantes centralizadas em `itbi/config.py` sem duplicação no restante do código

---

### Fase 1 — Módulos de etapa (extraídos de scraper.py)

Cada módulo deve ser executável standalone via `python -m itbi.<modulo>` além de importável.

- [x] [tier:medium] **1.1** `itbi/descoberta.py`
  - Função `descobrir_csv_urls(url: str = BASE_URL) -> dict[int, str]`
  - `if __name__ == "__main__"`: imprime URLs descobertas como JSON

- [x] [tier:medium] **1.2** `itbi/download.py`
  - Função `baixar_csvs(csv_urls: dict[int, str], destino: Path = DATA_DIR, anos: list[int] | None = None) -> list[Path]`
  - Parâmetro `anos` permite filtrar anos específicos
  - Cache: skip se arquivo já existe e `--force` não passado
  - `if __name__ == "__main__"`: aceita `--anos` via argparse

- [x] [tier:medium] **1.3** `itbi/consolidacao.py`
  - Função `carregar_e_consolidar(arquivos: list[Path]) -> pd.DataFrame`
  - Função `salvar_consolidado(df: pd.DataFrame, destino: Path = DATA_DIR) -> Path`
  - Limpeza: encoding, valores monetários, normalização de texto
  - `if __name__ == "__main__"`: lê CSVs do DATA_DIR, salva consolidado.csv

- [x] [tier:medium] **1.4** `itbi/geocodificacao.py`
  - Função `_montar_endereco(row: pd.Series) -> str`
  - Função `geocodificar(df: pd.DataFrame, cache_path: Path = GEOCACHE_CSV, reset_cache: bool = False) -> pd.DataFrame`
  - Parâmetro `reset_cache` para forçar re-geocodificação
  - Melhorar fallback: nível 1 = logradouro+bairro+cidade, nível 2 = só bairro+cidade, nível 3 = centroide do bairro (hardcoded para os ~50 bairros de Niterói)
  - `if __name__ == "__main__"`: lê consolidado.csv, salva consolidado_geo.csv

- [x] [tier:medium] **1.5** `itbi/heatmap.py`
  - Função `gerar_heatmap(df: pd.DataFrame, output_path: Path = OUTPUT_HTML, json_path: Path = DATA_JSON, incluir_marcadores: bool = True) -> None`
  - Parâmetro `incluir_marcadores` para desativar CircleMarkers (mapa mais leve)
  - `if __name__ == "__main__"`: lê consolidado_geo.csv, gera outputs

**Critério de aceite (DoD) da Fase 1**
- Cada módulo executa standalone via `python -m itbi.<modulo>` sem quebrar importações
- Cada módulo expõe funções com type hints e comportamento equivalente ao legado
- `scripts/scraper.py` permanece funcional durante migração (janela de compatibilidade)

---

### Fase 2 — CLI unificado

- [x] [tier:medium] **2.1** `itbi/cli.py` — CLI com argparse subcomandos:
  ```python
  parser = argparse.ArgumentParser(prog="itbi", description="Pipeline ITBI Niterói")
  subparsers = parser.add_subparsers(dest="comando", required=True)
  ```

- [x] [tier:medium] **2.2** Subcomando `run` — pipeline completo, flags opcionais:
  - `--anos 2022 2023 2024` — restringe download a anos específicos
  - `--skip-download` — assume CSVs já baixados
  - `--skip-geo` — assume consolidado_geo.csv já existe
  - `--no-markers` — gera mapa sem marcadores clicáveis

- [x] [tier:medium] **2.3** Subcomando `descobrir` — imprime URLs como tabela ou JSON (`--json`)

- [x] [tier:medium] **2.4** Subcomando `baixar` — flags `--anos`, `--force`

- [x] [tier:medium] **2.5** Subcomando `consolidar` — sem flags extras

- [x] [tier:medium] **2.6** Subcomando `geocodificar` — flags `--reset-cache`, `--limite N` (geocodifica só N endereços, útil para teste)

- [x] [tier:medium] **2.7** Subcomando `mapa` — flags `--no-markers`, `--output PATH`

- [x] [tier:medium] **2.8** Subcomando `status` — inspeciona artefatos:
  - Existe `consolidado.csv`? Quantas linhas? Última modificação?
  - Existe `geocache.csv`? Quantas entradas?
  - Existe `docs/index.html`? Última geração?

- [x] [tier:medium] **2.9** Subcomando `limpar` — remove CSVs baixados (`--tudo` inclui geocache com confirmação)

**Critério de aceite (DoD) da Fase 2**
- `itbi run` gera `docs/index.html` e `docs/data/itbi_geo.json` em execução limpa
- Subcomandos individuais executam sem dependências ocultas entre etapas
- Execução repetida sem dados novos não altera artefatos finais (idempotência funcional)

---

### Fase 3 — Testes

- [x] [tier:medium] **3.1** `tests/test_descoberta.py`
  - `test_descobrir_csv_urls_fallback` — mocka requests para falhar, verifica retorno do fallback
  - `test_descobrir_csv_urls_parse_html` — fornece HTML fixture, verifica extração de URLs

- [x] [tier:medium] **3.2** `tests/test_consolidacao.py`
  - `test_carregar_encoding_utf8` — CSV com encoding UTF-8 BOM
  - `test_carregar_encoding_latin1` — CSV com encoding latin-1
  - `test_limpeza_valores_monetarios` — verifica remoção de R$, pontos, vírgula→ponto
  - `test_normalizacao_colunas` — verifica `.str.upper()` e `.str.title()`

- [x] [tier:medium] **3.3** `tests/test_geocodificacao.py`
  - `test_montar_endereco` — verifica string montada corretamente
  - `test_geocodificar_usa_cache` — mocka Nominatim, verifica cache hit
  - `test_geocodificar_fallback_bairro` — mocka falha no endereço completo, verifica fallback

- [x] [tier:medium] **3.4** `tests/test_cli.py`
  - `test_status_sem_artefatos` — verifica saída quando nenhum CSV existe
  - `test_cli_run_dry` — mocka todas as etapas, verifica ordem de chamada

- [x] [tier:medium] **3.5** `tests/test_contratos_fonte.py`
  - `test_schema_csv_obrigatorio` — falha com mensagem clara se colunas mínimas mudarem
  - `test_parse_html_links_csv` — valida contrato de descoberta no HTML da fonte
  - `test_detecta_separador_e_encoding` — cobre variação de separador/encoding sem crash

- [x] [tier:medium] **3.6** `tests/test_idempotencia.py`
  - `test_pipeline_idempotente_sem_novos_dados` — duas execuções consecutivas não mudam outputs
  - `test_geocache_append_sem_duplicar` — reexecução não duplica entradas válidas no cache

- [x] [tier:medium] **3.7** `tests/test_recuperacao.py`
  - `test_geocache_corrompido_reconstrucao` — fallback para backup/rebuild com log de aviso
  - `test_timeout_parcial_geocodificacao_retomada` — retoma processamento sem perder progresso

**Critério de aceite (DoD) da Fase 3**
- Cobertura de cenários críticos: contrato da fonte, idempotência e recuperação
- Testes rodam em CI local (`pytest`) com resultado determinístico
- Falhas de contrato retornam erro acionável (sem mensagens genéricas)

---

### Fase 4 — Melhorias de visualização

- [x] [tier:medium] **4.1** Filtro por ano no mapa
  - Incluir `ANO DO PAGAMENTO DO ITBI` no JSON exportado por ponto
  - Adicionar slider HTML/JS customizado ao `index.html` via `folium.Element`
  - Ao mudar o slider, re-filtrar os pontos do HeatMap via JavaScript

- [x] [tier:medium] **4.2** Filtro por bairro (dropdown)
  - Adicionar select HTML com lista de bairros únicos
  - Filtrar pontos via JavaScript no lado cliente

- [x] [tier:fast] **4.3a** Baixar GeoJSON dos bairros de Niterói do SIGeo
- [x] [tier:medium] **4.3b** Choropleth por bairro
  - Agregar dados por bairro (valor_médio, total_transações)
  - Adicionar camada `folium.Choropleth` sobreposta ao heatmap
  - Toggle entre heatmap e choropleth via `LayerControl`

- [x] [tier:medium] **4.4** Painel de estatísticas
  - Card flutuante no canto superior direito do mapa
  - Exibe: total de transações, bairro mais ativo, valor médio global
  - Atualiza ao aplicar filtros de ano/bairro

**Critério de aceite (DoD) da Fase 4**
- Filtros de ano e bairro atualizam heatmap e painel sem recarregar a página
- Camadas (heatmap/choropleth) alternam sem inconsistência visual ou de dados
- Mapa permanece utilizável em desktop e mobile

---

### Fase 5 — Automação (GitHub Actions)

- [x] [tier:medium] **5.1** Workflow `.github/workflows/update.yml`
  - Schedule: `cron: "0 6 1 * *"` (todo dia 1 do mês às 6h UTC)
  - Steps: checkout → setup Python → pip install → `itbi run` (geocodificação incremental com cache)
  - Commit automático de `docs/` se houver mudanças (`git diff --quiet || git commit -m "auto: update heatmap"`)
  - Sem dados novos: workflow termina com sucesso e log explícito de "no changes"
  - Erros técnicos (rede, parse, schema) devem falhar o job com mensagem clara

- [x] [tier:heavy] **5.2** Estratégia de geocache no CI (decisão de design)
  - Opção A: GitHub Actions Cache (`actions/cache`) com chave por hash de código + período
  - Opção B: Artefato de workflow restaurado na execução seguinte (sem versionar `data/`)
  - **Decisão**: Opção A implementada — `key: geocache-${{ github.run_id }}` com `restore-keys: geocache-`

- [x] [tier:medium] **5.3** Badge de atualização no README
  - Badge de status do workflow GitHub Actions no topo do README
  - Link direto para o mapa publicado atualizado

**Critério de aceite (DoD) da Fase 5**
- Workflow mensal executa sem intervenção manual e publica apenas quando houver mudança
- Pipeline em CI é observável: logs distinguem "sem mudanças" de erro real
- Estratégia de cache não viola política de versionamento do projeto

---

### Fase 6 — Inteligência de valorização e "joias escondidas"

- [x] [tier:fast] **6.1** Levantar série IPCA oficial (fonte e formato) e definir mês-base do deflator

- [x] [tier:medium] **6.2** Criar camada analítica temporal por bairro e logradouro:
  - Métricas por janela (12/24/36 meses): `valor_total_real`, `qtd_transacoes`, `ticket_medio_real`, `variacao_real_%`
  - Aplicar piso de amostra (`min_transacoes`) para evitar conclusões frágeis

- [x] [tier:heavy] **6.3** Definir metodologia dos scores (fórmula inicial v0.1):
  - Normalização comum: `norm(x, lo, hi) = (clip(x, lo, hi) - lo) / (hi - lo)`
  - Base temporal (por região `r` e janela `W`):
    - `p0 = mediana(ticket_medio_real_mensal)` dos primeiros 3 meses ativos de `W`
    - `p1 = mediana(ticket_medio_real_mensal)` dos últimos 3 meses ativos de `W`
    - `trend_pct = (p1 / max(p0, eps)) - 1`
    - `trend_norm = norm(trend_pct, -0.20, 0.30)`
    - `q = qtd_transacoes_em_W`
    - `liquidez_norm = min(1, log1p(q) / log1p(120))`
    - `cv = std(ticket_medio_real_mensal) / max(mean(ticket_medio_real_mensal), eps)`
    - `estabilidade_norm = 1 - min(cv / 0.35, 1)`
  - Confiança do insight:
    - `c_amostra = min(1, q / 30)`
    - `c_cobertura = meses_ativos / meses_da_janela`
    - `c_geo = 1.0 (endereço) | 0.7 (bairro) | 0.4 (centroide)`
    - `confianca = 0.5*c_amostra + 0.3*c_cobertura + 0.2*c_geo`
  - `score_valorizacao` (0-100):
    - `raw_val = 0.55*trend_norm + 0.25*liquidez_norm + 0.20*estabilidade_norm`
    - `score_valorizacao = round(100 * raw_val * confianca, 1)`
  - `score_joia_escondida` (0-100):
    - `preco_ref = mediana(ticket_medio_real)` do benchmark (logradouro -> bairro, bairro -> cidade)
    - `desconto_pct = (preco_ref - p1) / max(preco_ref, eps)`
    - `desconto_norm = norm(desconto_pct, 0.00, 0.25)`
    - `q_prev6 = qtd_transacoes_6m_anteriores`, `q_last6 = qtd_transacoes_6m_recentes`
    - `liq_delta_pct = (q_last6 - q_prev6) / max(q_prev6, 1)`
    - `liq_delta_norm = norm(liq_delta_pct, -0.30, 0.50)`
    - `raw_joia = 0.40*trend_norm + 0.35*desconto_norm + 0.15*liq_delta_norm + 0.10*estabilidade_norm`
    - `score_joia_escondida = round(100 * raw_joia * confianca, 1)`
  - Regras de elegibilidade (evitar falso positivo):
    - Só rankear se `q >= 20`, `meses_ativos >= 6`, `confianca >= 0.55`
    - Para "joia escondida": exigir `trend_pct > 0` e `desconto_pct > 0`
    - Exibir selo: `alta` (`confianca >= 0.75`), `media` (`0.55-0.74`), `baixa` (`< 0.55`)

- [x] [tier:medium] **6.4** Gerar `docs/data/itbi_insights.json` com métricas, scores e nível de confiança

- [x] [tier:medium] **6.5** Criar `docs/insights.html`:
  - Mapa temático por região (bairro/rua)
  - Ranking de "joias escondidas"
  - Filtros por janela temporal e bairro

- [x] [tier:medium] **6.6** Transparência metodológica na interface:
  - Exibir fórmula resumida, limitações e selo de confiança (`alta`, `média`, `baixa`)
  - Destacar quando granularidade não permite leitura confiável em nível de quadra

- [x] [tier:medium] **6.7** Testes de qualidade analítica:
  - Reprodutibilidade dos scores (mesmo input -> mesmo output)
  - Validação de thresholds de amostra mínima
  - Verificação de estabilidade em reprocessamento mensal

- [x] [tier:medium] **6.8** Especificar pseudocódigo de implementação em `itbi/insights.py`

```python
# pseudocódigo (estrutura alvo)
from pathlib import Path
import json
import math
import pandas as pd

EPS = 1e-9

def norm(x: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    x_clip = min(max(x, lo), hi)
    return (x_clip - lo) / (hi - lo)

def selo_confianca(confianca: float) -> str:
    if confianca >= 0.75:
        return "alta"
    if confianca >= 0.55:
        return "media"
    return "baixa"

def calcular_confianca(q: int, meses_ativos: int, meses_janela: int, nivel_geo: str) -> float:
    c_amostra = min(1.0, q / 30)
    c_cobertura = meses_ativos / max(meses_janela, 1)
    c_geo = {"endereco": 1.0, "bairro": 0.7, "centroide": 0.4}.get(nivel_geo, 0.4)
    return 0.5 * c_amostra + 0.3 * c_cobertura + 0.2 * c_geo

def agregar_mensal(df: pd.DataFrame, nivel: str) -> pd.DataFrame:
    # 1) criar coluna ano_mes
    # 2) agrupar por [nivel, ano_mes]
    # 3) calcular qtd_transacoes_mes, valor_total_mes, ticket_medio_real_mensal
    return df_mensal

def extrair_features_janela(df_mensal: pd.DataFrame, meses_janela: int) -> pd.DataFrame:
    # por regiao:
    # - p0 / p1 via mediana dos 3 primeiros/ultimos meses ativos
    # - trend_pct, trend_norm
    # - q, liquidez_norm
    # - cv, estabilidade_norm
    # - q_prev6, q_last6, liq_delta_norm
    # - preco_ref, desconto_pct, desconto_norm
    return df_feat

def calcular_scores(df_feat: pd.DataFrame) -> pd.DataFrame:
    # raw_val = 0.55*trend_norm + 0.25*liquidez_norm + 0.20*estabilidade_norm
    # score_valorizacao = 100 * raw_val * confianca
    # raw_joia = 0.40*trend_norm + 0.35*desconto_norm + 0.15*liq_delta_norm + 0.10*estabilidade_norm
    # score_joia_escondida = 100 * raw_joia * confianca
    # aplicar elegibilidade: q>=20, meses_ativos>=6, confianca>=0.55
    # para joia: trend_pct>0 e desconto_pct>0
    return df_scores

def gerar_insights(consolidado_geo_csv: Path, output_json: Path) -> None:
    df = pd.read_csv(consolidado_geo_csv)
    # normalizar schema, aplicar deflator IPCA, marcar nivel_geo
    frames = []
    for nivel in ["logradouro", "bairro"]:
        df_mensal = agregar_mensal(df, nivel)
        for janela in [12, 24, 36]:
            df_feat = extrair_features_janela(df_mensal, janela)
            df_scores = calcular_scores(df_feat)
            df_scores["nivel"] = nivel
            df_scores["janela_meses"] = janela
            frames.append(df_scores)

    saida = pd.concat(frames, ignore_index=True)
    payload = {
        "metadata": {
            "versao_formula": "v0.1",
            "janelas": [12, 24, 36],
            "gerado_em": "<timestamp>",
        },
        "insights": saida.to_dict(orient="records"),
    }
    output_json.parent.mkdir(parents=True, exist_ok=True)
    output_json.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
```

- [x] [tier:heavy] **6.9** Definir mini-backtest para calibrar pesos/thresholds
  - Objetivo: calibrar pesos e limites sem overfitting e com sinal preditivo útil
  - Recorte temporal (walk-forward):
    - Treino: histórico até `T-12`
    - Validação: janela `T-12..T-7`
    - Teste cego: janela `T-6..T-1`
  - Universo avaliado: regiões com `q >= 20` e `meses_ativos >= 6`
  - Grade inicial de calibração:
    - Pesos `score_valorizacao`: testar variações de ±0.10 mantendo soma 1.0
    - Pesos `score_joia`: testar variações de ±0.10 mantendo soma 1.0
    - Thresholds: `confianca_min` {0.50, 0.55, 0.60}, `q_min` {15, 20, 30}
  - Métricas de avaliação:
    - `spearman_future_12m`: correlação entre score em `T` e variação real em `T+12m`
    - `precision_at_k`: proporção de acertos no top-k (k=10/20) para tendência positiva futura
    - `stability_tau`: estabilidade do ranking mês a mês (Kendall tau)
    - `coverage`: % de regiões elegíveis após filtros
  - Regra de escolha da configuração:
    - Maximizar `0.40*spearman + 0.30*precision_at_20 + 0.20*stability + 0.10*coverage`
    - Restrições: `coverage >= 0.25` e `stability_tau >= 0.60`
  - Entregáveis do backtest:
    - `docs/data/backtest_report.json` (métricas por configuração)
    - `docs/data/backtest_best_config.json` (pesos/thresholds escolhidos)
    - Registro no plano: atualização de `versao_formula` para `v0.2` quando calibrado

- [x] [tier:medium] **6.10** Normalização de endereços via LLM (Fireworks AI `kimi-k2p5`):
  - `itbi/normalizacao_llm.py`: normaliza endereços brutos em campos estruturados (logradouro, numero, bairro, municipio, estado)
  - Salva `data/itbi_niteroi/enderecos_normalizados.json` com cache incremental por batch
  - Consumido automaticamente por `_geocodificar_lote_geocodebr` quando o arquivo existe
  - Subcomando CLI: `itbi normalizar-enderecos [--batch-size 50] [--api-key KEY] [--output PATH]`
  - Env: `FIREWORKS_API_KEY`

**Critério de aceite (DoD) da Fase 6**
- Página `docs/insights.html` publicada com mapa + ranking + filtros funcionais
- Cada insight exibe nível de confiança e amostra utilizada
- Scores são reproduzíveis e auditáveis via `docs/data/itbi_insights.json`
- Comunicação evita afirmações categóricas sem base estatística
- Pseudocódigo de `itbi/insights.py` definido e rastreável para implementação
- Mini-backtest executado com configuração vencedora documentada

---

## 5. Riscos e mitigações

| Risco | Probabilidade | Mitigação |
|-------|---------------|-----------|
| URLs dos CSVs mudam | Baixa | Descoberta dinâmica + fallback hardcoded |
| Nominatim não resolve logradouros de Niterói | Média | Fallback em 3 níveis: logradouro → bairro → centroide fixo |
| Dados agregados limitam granularidade | Certa | Documentar limites; usar selo de confiança + amostra mínima; reavaliar LAI no marco semestral |
| Novo ano publicado sem re-run | Baixa | GitHub Actions com schedule mensal |
| Mapa com 5000+ marcadores fica lento | Média | Flag `--no-markers` para mapa leve; clustering via `MarkerCluster` |
| Geocache corrompido | Baixa | Nunca sobrescrever — só append; backup automático antes de `--reset-cache` |
| Fonte HTML/CSV muda sem aviso | Média | Testes de contrato + validação explícita de schema antes de processar |
| Regressão silenciosa em reruns | Média | Testes de idempotência + comparação de artefatos no CI |
| Tempo de execução acima do aceitável | Média | Definir SLO por etapa e alertar quando exceder limite |

---

## 6. Decisões de design

### Decisões fechadas (2026-02-20)
- **Packaging**: adotar `pyproject.toml` como padrão único de empacotamento/entry point
- **Marcadores no mapa**: manter ligados por padrão; em publicação automática, desativar com `--no-markers` quando volume de pontos exceder 5000
- **Geocache no CI**: usar `actions/cache` como estratégia primária e artefato restaurável como fallback
- **Política de falhas no agendamento**: "no changes" é sucesso com log explícito; erro técnico (rede/parse/schema) falha o job
- **Gate de qualidade**: os DoDs por fase são obrigatórios para avançar de fase
- **LAI (microdados)**: não solicitar nesta fase; reavaliar após medir valor da Fase 6 e necessidade de granularidade maior
- **Fallback geográfico**: adotar precisão em níveis (endereço, bairro, centroide) com selo de confiança no output
- **Clustering**: iniciar com `MarkerCluster`; migrar para JS puro apenas se performance ficar abaixo do aceitável
- **GeoJSON de bairros**: começar com fonte manual versionada e automatizar por API quando houver endpoint estável
- **Backtest (restrição de seleção)**: manter critério mínimo `coverage >= 0.25` e `stability_tau >= 0.60` alinhado entre plano e implementação

### Por que `argparse` em vez de `click`?
- Zero dependência extra — projeto já tem 6 dependências
- `click` valeria a pena se o CLI fosse muito grande (>10 subcomandos com muitas flags)
- Revisar se a complexidade crescer

### Por que pacote `itbi/` em vez de mais scripts em `scripts/`?
- `scripts/etapa1.py`, `scripts/etapa2.py` não resolve o problema de importações cruzadas
- Pacote com `__init__.py` permite `from itbi.config import DATA_DIR` em qualquer módulo
- `pyproject.toml` com entry point instala o comando `itbi` no PATH do virtualenv

### Geocache: append vs. rewrite
- Append garante que nunca se perde dados mesmo com crash no meio da geocodificação
- Deduplicação na leitura (último valor por endereço vence) resolve duplicatas eventuais

---

## 7. Perguntas em aberto (validadas)

1. **Granularidade (LAI)**: validada. Decisão atual é não abrir pedido LAI nesta fase devido lead time alto; reavaliar semestralmente.
2. **Geocodificação de qualidade**: validada. Fallback será em 3 níveis (endereço -> bairro -> centroide), sempre com nível de confiança explícito.
3. **Choropleth (SIGeo)**: validada com decisão provisória. Implementação inicia com GeoJSON manual versionado; automação por API fica condicionada à confirmação de endpoint estável.
4. **Clustering**: validada. Estratégia inicial com `MarkerCluster`; migração para JavaScript puro somente se benchmark de uso real indicar necessidade.

---

## 8. Referências

- Dados ITBI Niterói: https://www.fazenda.niteroi.rj.gov.br/site/dados-das-transacoes-imobiliarias/
- Dashboard Power BI: https://www.fazenda.niteroi.rj.gov.br/site/boletim-do-itbi/
- SIGeo Niterói (dados geoespaciais): https://www.sigeo.niteroi.rj.gov.br/pages/dados-abertos
- ToS Nominatim: https://operations.osmfoundation.org/policies/nominatim/
- folium docs: https://python-visualization.github.io/folium/
- argparse docs: https://docs.python.org/3/library/argparse.html
- IPCA/IBGE (SIDRA): https://sidra.ibge.gov.br/home/ipca
