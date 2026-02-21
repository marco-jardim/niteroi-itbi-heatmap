"""
Testes para itbi.heatmap — Fase 4.

Cobre:
- gerar_heatmap: saída HTML contém controles JS injetados
- gerar_heatmap: JSON exportado inclui campos ANO e QUANTIDADE
- gerar_heatmap: filtros JS populados com anos/bairros do DataFrame
- gerar_heatmap --no-markers: marcadores omitidos sem quebrar o mapa
- gerar_heatmap com geojson_bairros inexistente: choropleth omitido (warn)
- gerar_heatmap com geojson_bairros válido: choropleth adicionado
- _detect_col: localiza coluna por fragmento
- _safe_val: trata NaN, numpy int, numpy float, None
- _agregar_por_bairro: agrega por bairro corretamente
- _construir_pontos_js: produz JSON válido com campos corretos
- _construir_controles_filtro: substitui placeholder e contém painel HTML
"""

import json
from pathlib import Path

import pandas as pd
import pytest

from itbi.heatmap import (
    _agregar_por_bairro,
    _construir_controles_filtro,
    _construir_pontos_js,
    _detect_col,
    _safe_val,
    gerar_heatmap,
)


# ===========================================================================
# Fixture: DataFrame geocodificado mínimo
# ===========================================================================


@pytest.fixture
def df_geo() -> pd.DataFrame:
    """DataFrame geocodificado com todos os campos relevantes para Fase 4."""
    return pd.DataFrame(
        {
            "LAT": [-22.90, -22.91, -22.92],
            "LON": [-43.11, -43.12, -43.13],
            "BAIRRO": ["Icaraí", "Centro", "Icaraí"],
            "NOME DO LOGRADOURO": ["Rua A", "Rua B", "Rua C"],
            "NIVEL_GEO": ["endereco", "bairro", "endereco"],
            "VALOR DA TRANSAÇÃO": [500_000.0, 300_000.0, 700_000.0],
            "QUANTIDADE DE TRANSAÇÕES": [5.0, 3.0, 8.0],
            "ANO DO PAGAMENTO DO ITBI": [2022, 2023, 2024],
        }
    )


# ===========================================================================
# _detect_col
# ===========================================================================


def test_detect_col_encontra_coluna_por_fragmento(df_geo: pd.DataFrame) -> None:
    """Localiza coluna cujo nome contém o fragmento informado."""
    assert _detect_col(df_geo, "VALOR DA TRANSA") == "VALOR DA TRANSAÇÃO"


def test_detect_col_multiplos_fragmentos(df_geo: pd.DataFrame) -> None:
    """Localiza coluna quando todos os fragmentos estão presentes."""
    assert _detect_col(df_geo, "ANO", "PAGAMENTO") == "ANO DO PAGAMENTO DO ITBI"


def test_detect_col_nao_encontrada_retorna_none(df_geo: pd.DataFrame) -> None:
    """Retorna None se nenhuma coluna contiver o fragmento."""
    assert _detect_col(df_geo, "COLUNA_INEXISTENTE") is None


# ===========================================================================
# _safe_val
# ===========================================================================


def test_safe_val_none_retorna_none() -> None:
    assert _safe_val(None) is None


def test_safe_val_nan_retorna_none() -> None:
    import math

    assert _safe_val(float("nan")) is None


def test_safe_val_numpy_int_retorna_python_int() -> None:
    import numpy as np

    resultado = _safe_val(np.int64(42))
    assert resultado == 42
    assert type(resultado) is int


def test_safe_val_numpy_float_retorna_python_float() -> None:
    import numpy as np

    resultado = _safe_val(np.float64(3.14))
    assert abs(resultado - 3.14) < 1e-9  # type: ignore[operator]
    assert type(resultado) is float


def test_safe_val_python_nativo_passthrough() -> None:
    assert _safe_val(99) == 99
    assert _safe_val("texto") == "texto"
    assert _safe_val(3.14) == 3.14


# ===========================================================================
# _agregar_por_bairro
# ===========================================================================


def test_agregar_por_bairro_soma_quantidade(df_geo: pd.DataFrame) -> None:
    """Icaraí aparece 2 vezes: total deve ser 5+8=13."""
    # Adiciona PESO_NORM para a função (usada como fallback)
    df_geo = df_geo.copy()
    df_geo["PESO_NORM"] = 1.0

    resultado = _agregar_por_bairro(
        df_geo, "VALOR DA TRANSAÇÃO", "QUANTIDADE DE TRANSAÇÕES"
    )
    icarai = resultado[resultado["BAIRRO"] == "Icaraí"]
    assert not icarai.empty
    assert int(icarai["TOTAL_TRANSACOES"].iloc[0]) == 13


def test_agregar_por_bairro_valor_medio(df_geo: pd.DataFrame) -> None:
    """Valor médio do Centro deve ser 300000."""
    df_geo = df_geo.copy()
    df_geo["PESO_NORM"] = 1.0

    resultado = _agregar_por_bairro(
        df_geo, "VALOR DA TRANSAÇÃO", "QUANTIDADE DE TRANSAÇÕES"
    )
    centro = resultado[resultado["BAIRRO"] == "Centro"]
    assert not centro.empty
    assert abs(float(centro["VALOR_MEDIO"].iloc[0]) - 300_000.0) < 1.0


def test_agregar_por_bairro_sem_col_qtd_usa_contagem(df_geo: pd.DataFrame) -> None:
    """Sem coluna de quantidade, usa count de linhas por bairro."""
    df_geo = df_geo.copy()
    df_geo["PESO_NORM"] = 1.0

    resultado = _agregar_por_bairro(df_geo, None, None)
    assert "TOTAL_TRANSACOES" in resultado.columns
    icarai = resultado[resultado["BAIRRO"] == "Icaraí"]
    assert int(icarai["TOTAL_TRANSACOES"].iloc[0]) == 2  # 2 linhas de Icaraí


# ===========================================================================
# _construir_pontos_js
# ===========================================================================


def test_construir_pontos_js_retorna_json_valido(df_geo: pd.DataFrame) -> None:
    """Saída deve ser string JSON parseável."""
    df_geo = df_geo.copy()
    df_geo["PESO_NORM"] = 1.0

    js_str = _construir_pontos_js(
        df_geo, "VALOR DA TRANSAÇÃO", "QUANTIDADE DE TRANSAÇÕES"
    )
    pontos = json.loads(js_str)
    assert isinstance(pontos, list)
    assert len(pontos) == 3


def test_construir_pontos_js_contem_campos_fase4(df_geo: pd.DataFrame) -> None:
    """Pontos devem incluir 'ano' e 'qtd' além dos campos base."""
    df_geo = df_geo.copy()
    df_geo["PESO_NORM"] = 1.0

    pontos = json.loads(
        _construir_pontos_js(df_geo, "VALOR DA TRANSAÇÃO", "QUANTIDADE DE TRANSAÇÕES")
    )
    primeiro = pontos[0]
    assert "lat" in primeiro
    assert "lon" in primeiro
    assert "bairro" in primeiro
    assert "ano" in primeiro
    assert "qtd" in primeiro
    assert "valor_medio" in primeiro
    assert "peso_norm" in primeiro


def test_construir_pontos_js_sem_nan(df_geo: pd.DataFrame) -> None:
    """JSON não deve conter NaN (violaria JSON spec)."""
    df_geo = df_geo.copy()
    df_geo["PESO_NORM"] = 1.0
    df_geo.loc[0, "VALOR DA TRANSAÇÃO"] = float("nan")  # força NaN

    js_str = _construir_pontos_js(
        df_geo, "VALOR DA TRANSAÇÃO", "QUANTIDADE DE TRANSAÇÕES"
    )
    assert "NaN" not in js_str
    pontos = json.loads(js_str)  # deve parsear sem erro
    assert pontos[0]["valor_medio"] is None


# ===========================================================================
# _construir_controles_filtro
# ===========================================================================


def test_construir_controles_filtro_substitui_placeholder() -> None:
    """Placeholder __PONTOS__ deve ser substituído pelo JSON."""
    dummy_json = '[{"lat":-22.9,"lon":-43.1}]'
    html = _construir_controles_filtro(dummy_json)
    assert "__PONTOS__" not in html
    assert dummy_json in html


def test_construir_controles_filtro_contem_elementos_ui() -> None:
    """HTML deve conter os elementos de UI necessários."""
    html = _construir_controles_filtro("[]")
    assert 'id="itbi-panel"' in html
    assert 'id="itbi-ano"' in html
    assert 'id="itbi-bairro"' in html
    assert 'id="is-total"' in html
    assert 'id="is-bairro"' in html
    assert 'id="is-valor"' in html


def test_construir_controles_filtro_contem_js_funcoes_chave() -> None:
    """Script deve definir as funções JS fundamentais."""
    html = _construir_controles_filtro("[]")
    assert "findMap" in html
    assert "updateHeat" in html
    assert "updateStats" in html
    assert "setLatLngs" in html


# ===========================================================================
# gerar_heatmap — integração (sem I/O real de rede)
# ===========================================================================


def test_gerar_heatmap_cria_html_e_json(tmp_path: Path, df_geo: pd.DataFrame) -> None:
    """Arquivos de saída devem ser criados com conteúdo."""
    out_html = tmp_path / "index.html"
    out_json = tmp_path / "data" / "itbi_geo.json"

    gerar_heatmap(
        df_geo,
        output_path=out_html,
        json_path=out_json,
        incluir_marcadores=False,
    )

    assert out_html.exists()
    assert out_html.stat().st_size > 0
    assert out_json.exists()


def test_gerar_heatmap_html_contem_painel_filtros(
    tmp_path: Path, df_geo: pd.DataFrame
) -> None:
    """HTML gerado deve conter o painel de filtros injetado."""
    out_html = tmp_path / "index.html"
    gerar_heatmap(
        df_geo,
        output_path=out_html,
        json_path=tmp_path / "d.json",
        incluir_marcadores=False,
    )

    html = out_html.read_text(encoding="utf-8")
    assert "itbi-panel" in html
    assert "itbi-ano" in html
    assert "itbi-bairro" in html
    assert "updateHeat" in html
    assert "is-total" in html


def test_gerar_heatmap_json_inclui_ano_e_quantidade(
    tmp_path: Path, df_geo: pd.DataFrame
) -> None:
    """JSON exportado deve incluir campo de ano e quantidade de transações."""
    out_json = tmp_path / "itbi_geo.json"
    gerar_heatmap(
        df_geo,
        output_path=tmp_path / "i.html",
        json_path=out_json,
        incluir_marcadores=False,
    )

    records = json.loads(out_json.read_text(encoding="utf-8"))
    primeiro = records[0]
    # Deve conter o campo de ano
    assert any("ANO" in k for k in primeiro.keys())
    # Deve conter o campo de quantidade
    assert any("QUANTIDADE" in k for k in primeiro.keys())


def test_gerar_heatmap_json_pontos_js_contem_bairros(
    tmp_path: Path, df_geo: pd.DataFrame
) -> None:
    """HTML deve conter bairros do DataFrame no array JS de pontos."""
    out_html = tmp_path / "index.html"
    gerar_heatmap(
        df_geo,
        output_path=out_html,
        json_path=tmp_path / "d.json",
        incluir_marcadores=False,
    )

    html = out_html.read_text(encoding="utf-8")
    assert "Icaraí" in html
    assert "Centro" in html


def test_gerar_heatmap_no_markers_omite_circlemarker(
    tmp_path: Path, df_geo: pd.DataFrame
) -> None:
    """Com --no-markers, HTML não deve conter CircleMarker."""
    out_html = tmp_path / "index.html"
    gerar_heatmap(
        df_geo,
        output_path=out_html,
        json_path=tmp_path / "d.json",
        incluir_marcadores=False,
    )

    html = out_html.read_text(encoding="utf-8")
    assert "CircleMarker" not in html


def test_gerar_heatmap_geojson_inexistente_nao_quebra(
    tmp_path: Path, df_geo: pd.DataFrame
) -> None:
    """GeoJSON inexistente deve gerar aviso mas não levantar exceção."""
    out_html = tmp_path / "index.html"
    geojson_path = tmp_path / "bairros_inexistente.geojson"

    gerar_heatmap(
        df_geo,
        output_path=out_html,
        json_path=tmp_path / "d.json",
        incluir_marcadores=False,
        geojson_bairros=geojson_path,
    )

    # Mapa ainda deve ser gerado
    assert out_html.exists()


def test_gerar_heatmap_choropleth_com_geojson_valido(
    tmp_path: Path, df_geo: pd.DataFrame
) -> None:
    """GeoJSON válido deve resultar em HTML com elementos do choropleth."""
    # GeoJSON mínimo com os bairros do df_geo
    geojson_data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"nome": "Icaraí"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-43.12, -22.90],
                            [-43.11, -22.90],
                            [-43.11, -22.91],
                            [-43.12, -22.91],
                            [-43.12, -22.90],
                        ]
                    ],
                },
            },
            {
                "type": "Feature",
                "properties": {"nome": "Centro"},
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [
                            [-43.13, -22.91],
                            [-43.12, -22.91],
                            [-43.12, -22.92],
                            [-43.13, -22.92],
                            [-43.13, -22.91],
                        ]
                    ],
                },
            },
        ],
    }
    geojson_path = tmp_path / "bairros.geojson"
    geojson_path.write_text(json.dumps(geojson_data), encoding="utf-8")

    out_html = tmp_path / "index.html"
    gerar_heatmap(
        df_geo,
        output_path=out_html,
        json_path=tmp_path / "d.json",
        incluir_marcadores=False,
        geojson_bairros=geojson_path,
        choropleth_key="nome",
    )

    html = out_html.read_text(encoding="utf-8")
    # Choropleth gera uma legenda e usa YlOrRd
    assert "YlOrRd" in html or "choropleth" in html.lower()


def test_gerar_heatmap_layer_control_presente(
    tmp_path: Path, df_geo: pd.DataFrame
) -> None:
    """LayerControl deve estar presente no HTML gerado."""
    out_html = tmp_path / "index.html"
    gerar_heatmap(
        df_geo,
        output_path=out_html,
        json_path=tmp_path / "d.json",
        incluir_marcadores=False,
    )

    html = out_html.read_text(encoding="utf-8")
    # Folium LayerControl gera um div com leaflet-control-layers
    assert "leaflet-control-layers" in html or "LayerControl" in html


# ===========================================================================
# CLI — flags de choropleth
# ===========================================================================


def test_cli_mapa_aceita_flag_choropleth_geojson(tmp_path: Path) -> None:
    """--choropleth-geojson deve ser aceito pelo subparser mapa sem erro."""
    from itbi.cli import _build_parser

    parser = _build_parser()
    args = parser.parse_args(
        ["mapa", "--choropleth-geojson", str(tmp_path / "bairros.geojson")]
    )
    assert args.choropleth_geojson == str(tmp_path / "bairros.geojson")


def test_cli_mapa_aceita_flag_choropleth_key(tmp_path: Path) -> None:
    """--choropleth-key deve ser aceito pelo subparser mapa."""
    from itbi.cli import _build_parser

    parser = _build_parser()
    args = parser.parse_args(["mapa", "--choropleth-key", "nome_bairro"])
    assert args.choropleth_key == "nome_bairro"


def test_cli_run_aceita_flags_choropleth() -> None:
    """--choropleth-geojson e --choropleth-key devem ser aceitos pelo subparser run."""
    from itbi.cli import _build_parser

    parser = _build_parser()
    args = parser.parse_args(
        ["run", "--choropleth-geojson", "bairros.geojson", "--choropleth-key", "nome"]
    )
    assert args.choropleth_geojson == "bairros.geojson"
    assert args.choropleth_key == "nome"
