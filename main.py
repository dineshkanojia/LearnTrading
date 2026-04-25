# main.py
"""
Thin entrypoint shim. Real work lives in engine.cli.

Run with:
    python main.py
    python main.py --verbose --start "15 Dec 2025 00:00:00"
"""

import sys

from engine.cli import main

if __name__ == "__main__":
    sys.exit(main())
