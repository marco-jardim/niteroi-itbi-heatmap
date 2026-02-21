"""
Ponto de entrada de ``python -m itbi``.

Delega imediatamente para :func:`itbi.cli.main`, que constrói o parser
argparse e despacha para o subcomando correto.

Uso::

    python -m itbi --help
    python -m itbi run
    python -m itbi status
"""

from itbi.cli import main

if __name__ == "__main__":
    main()
