# niteroi-itbi-heatmap

Mapa de calor dos valores de transações imobiliárias em Niterói/RJ, baseado nos dados públicos de ITBI da Secretaria Municipal de Fazenda.

**[Ver mapa →](https://seu-usuario.github.io/niteroi-itbi-heatmap)**

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

# Executar pipeline completo
python scripts/scraper.py
```

O script:
1. Descobre dinamicamente os CSVs na página da Fazenda
2. Faz download (com cache local — não re-baixa se já existir)
3. Consolida e limpa os dados
4. Geocodifica via Nominatim/OSM (com cache em `data/itbi_niteroi/geocache.csv`)
5. Gera `docs/index.html` (heatmap) e `docs/data/itbi_geo.json`

A geocodificação respeita o rate limit de 1 req/s do Nominatim. Para ~500 logradouros únicos, espere ~10 minutos na primeira execução. Execuções subsequentes usam o cache.

## Estrutura

```
niteroi-itbi-heatmap/
  scripts/
    scraper.py          # pipeline completo
  docs/
    index.html          # heatmap (GitHub Pages)
    data/
      itbi_geo.json     # dados pré-processados
  data/                 # não versionado (.gitignore)
    itbi_niteroi/
      transacoes_imobiliarias_YYYY.csv
      consolidado.csv
      consolidado_geo.csv
      geocache.csv
  PLAN.md               # plano de implementação
  requirements.txt
```

## Tecnologias

- **requests + BeautifulSoup** — coleta dos CSVs
- **pandas** — limpeza e consolidação
- **geopy (Nominatim/OSM)** — geocodificação
- **folium** — geração do heatmap interativo
- **GitHub Pages** — publicação do mapa

## Publicação

O mapa é gerado localmente e o `docs/` é commitado e servido pelo GitHub Pages.
Não há backend — tudo é HTML/JS estático.

Para atualizar:
```bash
python scripts/scraper.py
git add docs/
git commit -m "update: heatmap AAAA-MM-DD"
git push
```

## Licença

Dados: domínio público (Prefeitura de Niterói).
Código: MIT.
