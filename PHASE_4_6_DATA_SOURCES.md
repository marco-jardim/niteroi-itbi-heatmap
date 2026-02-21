# Data Sources Research — PLAN Phase 4.3a & 6.1

**Research Date**: 2026-02-20  
**Status**: Exploration only, no implementation  

---

## Phase 4.3a — Niterói Bairros GeoJSON (Choropleth Layer)

### Primary Source: SIGeo Niterói (ArcGIS Hub)

**URL**: https://www.sigeo.niteroi.rj.gov.br/pages/dados-abertos

**Status**: ✅ ACTIVE & PUBLIC  
**Format**: ArcGIS REST API + GeoJSON export available  
**Licensing**: Public domain (municipal open data)

### Discovery Path

1. **Main Portal**: https://www.sigeo.niteroi.rj.gov.br/pages/dados-abertos
   - Category: "Limites Políticos" (Political Boundaries)
   - Available formats: GeoJSON, SHP, REST API
   - Direct download via ArcGIS Hub interface

2. **REST API Endpoint Pattern** (inferred from ArcGIS Hub):
   ```
   https://www.sigeo.niteroi.rj.gov.br/arcgis/rest/services/[LayerName]/FeatureServer/[LayerID]/query
   ?where=1=1
   &outSR={"wkid":4326}
   &f=geojson
   ```

3. **Alternative: Direct GeoJSON Export**
   - Each dataset has a download button with multiple formats
   - For bairros: look for layer named "Limites Administrativos de Bairros"
   - Expected ~50 polygon features (Niterói has ~52 bairros)

### Implementation Recommendations

**Storage Location**:
```
data/
└── bairros_niteroi/
    ├── bairros.geojson          # Raw download (committed for stability)
    ├── bairros_simplified.geojson # Simplified for web (optional)
    └── bairros_metadata.json     # Downloaded date + source URL
```

**Retrieval Strategy**:
- **Phase 4.3b** (near-term): Manual download → version control in repo
- **Phase 5.3+** (GitHub Actions): Auto-fetch via REST API endpoint with ETag caching
- **Fallback**: Keep committed GeoJSON; document manual update process

**Expected Schema**:
```json
{
  "type": "FeatureCollection",
  "features": [
    {
      "type": "Feature",
      "properties": {
        "NOME": "Icaraí",
        "BAIRRO_ID": "001",
        "AREA_KM2": 1.234
      },
      "geometry": { "type": "Polygon", "coordinates": [...] }
    }
  ]
}
```

**Key Fields for Choropleth**:
- `NOME` → neighborhood name (join key with ITBI `BAIRRO` column)
- `geometry.coordinates` → polygon boundary for `folium.Choropleth`

---

## Phase 6.1 — IPCA Monthly Series (Deflation Index)

### Primary Source: IBGE SIDRA

**Portal**: https://sidra.ibge.gov.br/home/ipca  
**Status**: ✅ STABLE & OFFICIAL  
**Data Provider**: Instituto Brasileiro de Geografia e Estatística (IBGE)  
**Update Frequency**: Monthly (released ~10th of following month)

### Table Options

| Table ID | Name | Coverage | Format |
|----------|------|----------|--------|
| **1737** | IPCA — Full historical series | Dec 1979 → present | Index + monthly % change |
| 118 | IPCA seasonally adjusted | Jan 1998 → present | Monthly variation only |
| 7060 | IPCA by category/territory | Jan 2020 → present | CSV, supports regional breakdown |
| 6691 | Recent IPCA (short series) | Nov 2014 → present | Lightweight option |

### Recommended: Table 1737 (Full Historical)

**Direct Download Link Pattern**:
```
https://sidra.ibge.gov.br/tabela/1737
→ Custom export → XLSX/ODS/JSON
```

### Machine-Readable Retrieval (No Official REST API)

**Option A: Web Scraping (fragile, requires updates)**
```python
# Example query structure observed on SIDRA
url = "https://sidra.ibge.gov.br/tabela/1737"
# Manual selection of date range, export as XLSX
# Parse locally
```

**Option B: IBGE Public FTP** (if available)
- Check: https://ftp.ibge.gov.br/Precos_e_Indices_de_Precos/
- IPCA series sometimes published as raw CSV/TXT

**Option C: Third-party JSON APIs** (use with caution)
- https://economia.awesomeapi.com.br/json/available?token=economia
- Provides cached IPCA data (verify freshness before use)

### Recommended Implementation Approach

**Store IPCA locally for reproducibility**:

```
data/
└── ipca_niteroi/
    ├── ipca_series.csv          # Downloaded from SIDRA table 1737
    ├── ipca_metadata.json        # {"source": "SIDRA", "table_id": 1737, "last_update": "2026-02-10"}
    └── deflation.log             # Which months were used for each run
```

**CSV Schema** (post-download, manual cleanup):
```
month_year,ipca_index,ipca_monthly_pct,ipca_12m_pct
2020-01,97.654,0.42,-0.15
2020-02,98.102,0.45,0.20
...
2026-01,125.432,0.33,3.42
```

### Field Mapping for Phase 6.2+

**For deflation calculation**:
```python
# Example: convert 2020 ITBI value to Jan 2026 reais
base_month = "2020-01"  # Reference month (adjustable)
target_month = "2026-01"

ipca_base = ipca_data.loc[ipca_data['month_year'] == base_month, 'ipca_index'].iloc[0]
ipca_target = ipca_data.loc[ipca_data['month_year'] == target_month, 'ipca_index'].iloc[0]

deflator = ipca_target / ipca_base
valor_real = valor_nominal * deflator
```

### Data Quality Notes

✅ **Strengths**:
- Official government source (IBGE = authoritative)
- Published since 1979 (long history for back-testing)
- Monthly granularity sufficient for 12/24/36-month windows (Phase 6.3)
- No authentication required

⚠ **Caveats**:
- No dedicated machine-readable REST API (requires manual download)
- Revised in subsequent releases (keep historical snapshots)
- Specific to consumer price inflation (not property market inflation, but standard Brazilian deflator)

---

## Integration Timeline

### Phase 4.3a (Now - Sprint)
- [ ] Download bairros GeoJSON from SIGeo
- [ ] Commit to `data/bairros_niteroi/bairros.geojson`
- [ ] Verify schema: ≥50 features, NOME field present
- [ ] Document manual refresh process in README

### Phase 6.1 (Near-term)
- [ ] Download IPCA table 1737 from SIDRA
- [ ] Parse & normalize to `data/ipca_niteroi/ipca_series.csv`
- [ ] Define base month for deflation (recommend: 2020-01 or 2024-01)
- [ ] Commit both `itbi/config.py` with base month constant

### Phase 5.2+ (GitHub Actions, future)
- [ ] Add optional auto-fetch for IPCA (check monthly)
- [ ] Cache GeoJSON with ETag validation
- [ ] Alert if download fails (fail workflow only if schema validation breaks)

---

## Summary: Practical Integration Notes

### File Structure (Target)
```
niteroi-itbi-heatmap/
├── data/
│   └── bairros_niteroi/
│       ├── bairros.geojson               # ~500 KB
│       ├── bairros_simplified.geojson    # Optional: for web (simplified ~30%)
│       └── bairros_metadata.json
│   └── ipca_niteroi/
│       ├── ipca_series.csv               # ~10 KB
│       └── ipca_metadata.json
├── itbi/
│   ├── config.py                         # IPCA_BASE_MONTH, IPCA_PATH
│   ├── insights.py                       # Import & use ipca_series for deflation
│   └── visualization.py                  # Import & use bairros.geojson for choropleth
```

### Code Snippets (Template)

**Config**:
```python
# itbi/config.py
IPCA_PATH = DATA_DIR / "ipca_niteroi" / "ipca_series.csv"
BAIRROS_GEOJSON = DATA_DIR / "bairros_niteroi" / "bairros.geojson"
IPCA_BASE_MONTH = "2020-01"  # Reference for deflation
```

**Load IPCA**:
```python
# itbi/insights.py or itbi/heatmap.py
import pandas as pd

def load_ipca_deflator(csv_path: Path, base_month: str) -> dict:
    df = pd.read_csv(csv_path)
    df['month_year'] = pd.to_datetime(df['month_year'])
    base_idx = df[df['month_year'].dt.strftime('%Y-%m') == base_month]['ipca_index'].iloc[0]
    deflators = {}
    for _, row in df.iterrows():
        month_key = row['month_year'].strftime('%Y-%m')
        deflators[month_key] = row['ipca_index'] / base_idx
    return deflators

ipca_deflators = load_ipca_deflator(IPCA_PATH, IPCA_BASE_MONTH)
```

**Load Bairros GeoJSON**:
```python
# itbi/visualization.py
import json

def load_bairros_geojson(geojson_path: Path) -> dict:
    with open(geojson_path, encoding='utf-8') as f:
        return json.load(f)

bairros = load_bairros_geojson(BAIRROS_GEOJSON)
# Use with: folium.Choropleth(geo_data=bairros, ...)
```

---

## Licensing & Attribution

### Bairros GeoJSON (SIGeo)
- **License**: Public domain (municipal open data)
- **Attribution**: "Prefeitura de Niterói — SIGeo"
- **Terms**: Free use, cite source on visualizations

### IPCA Data (IBGE/SIDRA)
- **License**: Public domain (federal statistical agency)
- **Attribution**: "IBGE — SIDRA — Tabela 1737"
- **Terms**: No restrictions on non-commercial use

---

## Validation Checklist

Before implementation, verify:

- [ ] bairros.geojson loads in Python: `json.load(f)["features"] | len ≥ 50`
- [ ] All features have `properties.NOME` field (exact case from SIGeo)
- [ ] ipca_series.csv has columns: `month_year`, `ipca_index`, `ipca_monthly_pct`
- [ ] IPCA data spans ≥2020-01 to current month
- [ ] Test deflation math: `ipca_idx_2026 / ipca_idx_2020 ≈ 1.25`

---

## Open Questions for Implementation Phase

1. **Bairros naming**: Does SIGeo `NOME` exactly match ITBI `BAIRRO` column? (Need sample comparison)
2. **IPCA refresh cadence**: Auto-download monthly in GitHub Actions, or manual updates only?
3. **Deflation base month**: 2020-01 (start of ITBI series) or 2024-01 (recent)? (Affects comparability)
4. **GeoJSON simplification**: Keep full resolution or reduce for web performance? (Test file size in folium)

