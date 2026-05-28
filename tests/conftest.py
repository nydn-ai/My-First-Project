"""
pytest configuration for the vertex-ml-logs pipeline test suite.

Adds the project root to sys.path so that `pipeline.*` and `scripts.*`
imports resolve correctly when pytest is run from any directory.
"""

import sys
from pathlib import Path

# Ensure project root is first on sys.path
ROOT = Path(__file__).parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
