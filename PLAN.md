# PLAN — niteroi-itbi-heatmap

> Mapa de calor dos valores de imóveis transacionados em Niterói, com base nos dados públicos de ITBI da Secretaria Municipal de Fazenda.

---

## 1. Visão geral

### Objetivo
Coletar, geocodificar e visualizar as médias de valores de transações imobiliárias (ITBI) por logradouro em Niterói/RJ, publicadas como dados abertos pela Prefeitura.

### Arquitetura de publicação
```
[Seu PC] scraper.py → dados processados (CSV + JSON)
                              ↓
                      docs/index.html  ←── GitHub Pages (público)
                      docs/data/itbi_geo.json
```

**Por que essa arquitetura?**
- Processamento pesado (geocodificação ~minutosa) roda localmente
- GitHub Pages serve só HTML estático + JSON pré-processado → zero custo, zero backend
- O repositório público documenta a metodologia e os dados

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

## 3. Fases de implementação

### Fase 1 — Coleta e processamento (local) ✅ Draft pronto
**Arquivo:** `scripts/scraper.py`

| Etapa | Descrição | Tecnologia |
|-------|-----------|------------|
| 1. Descoberta | Parse da página WordPress, extrai links `.csv` via regex | `requests` + `BeautifulSoup` |
| 2. Download | Baixa CSVs com cache simples (skip se existir) | `requests` |
| 3. Limpeza | Normaliza encoding (UTF-8 BOM / latin-1), valores monetários, nomes | `pandas` |
| 4. Geocodificação | Monta endereço "Logradouro, Bairro, Niterói, RJ, Brasil" → lat/lon | `geopy` (Nominatim/OSM) |
| 5. Heatmap | Mapa interativo com peso = valor_médio × qtd_transações | `folium` |

**Saídas:**
- `data/itbi_niteroi/consolidado.csv` — dados limpos
- `data/itbi_niteroi/consolidado_geo.csv` — com lat/lon
- `data/itbi_niteroi/geocache.csv` — cache de geocodificação (não commitar)
- `docs/index.html` — heatmap para GitHub Pages
- `docs/data/itbi_geo.json` — dados pré-processados

---

### Fase 2 — GitHub Pages

**Estrutura `docs/`:**
```
docs/
  index.html          ← gerado pelo scraper (Folium)
  data/
    itbi_geo.json     ← pontos geocodificados com pesos
```

**Configuração:**
1. No GitHub: Settings → Pages → Branch `main`, pasta `/docs`
2. O `index.html` do Folium é auto-suficiente (CSS/JS embutidos)

**`.gitignore` para dados:**
```
data/              # dados brutos (CSV baixados)
__pycache__/
*.pyc
.env
```

**O que commitar:**
- `docs/index.html` — o mapa final
- `docs/data/itbi_geo.json` — dados processados (sem PII, são dados públicos)
- `scripts/scraper.py` — código fonte
- `requirements.txt`

---

### Fase 3 — Melhorias futuras

| Prioridade | Melhoria | Esforço |
|------------|----------|---------|
| Alta | Filtro por ano no mapa (slider JS) | Médio |
| Alta | Filtro por bairro (dropdown) | Baixo |
| Média | Choropleth por bairro (GeoJSON dos bairros de Niterói) | Médio |
| Média | Dados individuais via LAI (Lei de Acesso à Informação) | Alto |
| Baixa | Dashboard Power BI embed complementar | Baixo |
| Baixa | Comparativo com dados do Rio de Janeiro | Alto |

---

## 4. Riscos e mitigações

| Risco | Probabilidade | Mitigação |
|-------|---------------|-----------|
| URLs dos CSVs mudam | Baixa | Descoberta dinâmica + fallback hardcoded |
| Nominatim não resolve logradouros de Niterói | Média | Fallback por bairro; cache persistente; considerar HERE API |
| Dados agregados limitam granularidade | Certa | Documentar claramente no README e no mapa; solicitar microdados via LAI |
| Novo ano publicado sem re-run | Baixa | Cron local ou GitHub Actions agendado |

---

## 5. Perguntas em aberto

1. **Granularidade**: vale solicitar microdados via LAI para complementar?
2. **Geocodificação de qualidade**: logradouros sem número podem errar bairro — aceitar imprecisão ou usar fallback centroide do bairro?
3. **Atualização**: rodar manualmente quando sair CSV 2025, ou automatizar via GitHub Actions com schedule?
4. **Visualização complementar**: adicionar choropleth por bairro usando SIGeo (https://www.sigeo.niteroi.rj.gov.br)?

---

## 6. Referências

- Dados ITBI Niterói: https://www.fazenda.niteroi.rj.gov.br/site/dados-das-transacoes-imobiliarias/
- Dashboard Power BI: https://www.fazenda.niteroi.rj.gov.br/site/boletim-do-itbi/
- SIGeo Niterói (dados geoespaciais): https://www.sigeo.niteroi.rj.gov.br/pages/dados-abertos
- ToS Nominatim: https://operations.osmfoundation.org/policies/nominatim/
