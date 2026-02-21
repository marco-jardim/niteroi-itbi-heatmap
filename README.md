# niteroi-itbi-heatmap

[![Workflow: update](https://img.shields.io/badge/workflow-update.yml-blue)](.github/workflows/update.yml)

Mapa de calor dos valores de transações imobiliárias em Niterói/RJ, baseado nos dados públicos de ITBI da Secretaria Municipal de Fazenda.

**[🗺 Ver mapa local (docs/index.html) →](docs/index.html)**

URL pública no GitHub Pages (após publicar no seu repositório):
`https://<owner>.github.io/niteroi-itbi-heatmap`

---

## O que é

Visualização interativa das médias de valores de compra e venda de imóveis por logradouro em Niterói, usando os dados abertos de ITBI (Imposto sobre Transmissão de Bens Imóveis) publicados pela Prefeitura.

O mapa de calor mostra a intensidade do volume financeiro de transações (valor médio × quantidade), de azul (menor) a vermelho (maior). Clique em qualquer marcador para ver detalhes do logradouro.

## Fonte dos dados

**Secretaria Municipal de Fazenda de Niterói**
https://www.fazenda.niteroi.rj.gov.br/site/dados-das-transacoes-imobiliarias/

- 5 arquivos CSV, um por ano (2020–2024)
- Dados agregados por logradouro (não transações individuais)
- 11 campos: bairro, logradouro, áreas (lote/edificada/privativa), valores (avaliação/transação), quantidade, tipologia, natureza, ano

> **Limitação importante**: os dados são médias por logradouro, não registros individuais. O mapa mostra "rua com maior volume financeiro", não "apartamento mais caro".

## Como rodar localmente

```bash
# Instalar dependências
pip install -r requirements.txt

# Pipeline completo (descobrir → baixar → consolidar → geocodificar → mapa)
python -m itbi run

# Etapas individuais (útil para re-executar apenas parte do pipeline)
python -m itbi descobrir          # lista URLs dos CSVs disponíveis
python -m itbi baixar             # faz download dos CSVs anuais
python -m itbi consolidar         # une CSVs em consolidado.csv
python -m itbi geocodificar       # geocodifica via Nominatim (com cache)
python -m itbi geocodificar --geocoder geocodebr  # usa geocodebr (R) em lote
python -m itbi run --geocoder auto  # tenta geocodebr, cai para Nominatim
python -m itbi mapa               # gera docs/index.html e itbi_geo.json

# Inspecionar estado dos artefatos
python -m itbi status
```

O pipeline:
1. Descobre dinamicamente os CSVs na página da Fazenda (com fallback hardcoded)
2. Faz download (com cache local — não re-baixa se já existir)
3. Consolida e limpa os dados
4. Geocodifica via Nominatim/OSM ou geocodebr (R), com cache incremental em `data/itbi_niteroi/geocache.csv`
5. Gera `docs/index.html` (heatmap interativo) e `docs/data/itbi_geo.json`

A geocodificação com Nominatim respeita o rate limit de 1 req/s. Para ~500 logradouros únicos, espere ~10 minutos na primeira execução. Execuções subsequentes usam o cache e são instantâneas.

Para usar `geocodebr`, é necessário ter `Rscript` no PATH e o pacote R instalado:

```bash
Rscript -e "install.packages('geocodebr', repos='https://cloud.r-project.org')"
```

## Estrutura

```
niteroi-itbi-heatmap/
  itbi/                       # pacote Python (CLI + pipeline)
    __main__.py               # python -m itbi
    cli.py                    # subcomandos: run, descobrir, baixar, ...
    config.py                 # constantes e caminhos centralizados
    descoberta.py             # etapa 1: descoberta dinâmica de URLs
    download.py               # etapa 2: download de CSVs com cache
    consolidacao.py           # etapa 3: limpeza e consolidação
    geocodificacao.py         # etapa 4: geocodificação Nominatim
    heatmap.py                # etapa 5: geração do mapa Folium
  .github/
    workflows/
      update.yml              # automação mensal (GitHub Actions)
  docs/                       # GitHub Pages (versionado)
    index.html                # heatmap interativo
    data/
      itbi_geo.json           # dados pré-processados
  data/                       # cache local — NÃO versionado (.gitignore)
    itbi_niteroi/
      transacoes_imobiliarias_YYYY.csv
      consolidado.csv
      consolidado_geo.csv
      geocache.csv            # cache de geocodificação — nunca deletar
  tests/                      # suite de testes (pytest)
  PLAN.md                     # arquitetura e roadmap
  pyproject.toml
  requirements.txt
```

## Tecnologias

- **requests + BeautifulSoup** — coleta dos CSVs
- **pandas** — limpeza e consolidação
- **geopy (Nominatim/OSM)** — geocodificação
- **folium** — geração do heatmap interativo
- **GitHub Pages** — publicação do mapa

## Publicação

O mapa é servido pelo GitHub Pages a partir do diretório `docs/` — sem backend,
tudo é HTML/JS estático.

**Automação mensal:** o workflow [`.github/workflows/update.yml`](.github/workflows/update.yml)
executa todo dia 1 do mês às 06:00 UTC, roda o pipeline completo e commita
`docs/` apenas quando houver dados novos. Se não houver mudanças, o job encerra
com sucesso e registra "no changes" no log.

Para atualizar manualmente:
```bash
python -m itbi run
git add docs/
git commit -m "update: heatmap AAAA-MM-DD"
git push
```

## Licença

Dados: domínio público (Prefeitura de Niterói).
Código: MIT.
