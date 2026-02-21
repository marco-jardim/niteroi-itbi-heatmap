"""
Pacote ITBI Niterói — pipeline de coleta, geocodificação e visualização.

Módulos disponíveis (cada um executável via ``python -m itbi.<modulo>``):

- ``itbi.config``         — constantes e caminhos centralizados
- ``itbi.descoberta``     — Etapa 1: descoberta dinâmica de URLs CSV
- ``itbi.download``       — Etapa 2: download dos CSVs anuais com cache
- ``itbi.consolidacao``   — Etapa 3: limpeza e consolidação dos dados
- ``itbi.geocodificacao`` — Etapa 4: geocodificação via Nominatim
- ``itbi.heatmap``        — Etapa 5: geração do mapa interativo Folium
- ``itbi.insights``       — Etapa 6: valorização e joias escondidas
- ``itbi.backtest``       — Backtest de calibração de pesos/thresholds
"""

__version__ = "0.1.0"
