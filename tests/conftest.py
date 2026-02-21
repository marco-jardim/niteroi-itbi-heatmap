"""
conftest.py — Fixtures e configurações globais para os testes.

Aplicado automaticamente a todos os módulos de teste (autouse=True):
- tqdm substituído por iteração direta (sem saída de progresso nos testes).
"""

import pytest


@pytest.fixture(autouse=True)
def desabilitar_tqdm(monkeypatch: pytest.MonkeyPatch) -> None:
    """Substitui tqdm por passthrough para suprimir barras de progresso."""
    monkeypatch.setattr(
        "itbi.geocodificacao.tqdm",
        lambda iterable, **kw: iterable,
    )
