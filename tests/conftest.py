import sys
from pathlib import Path

# Allows running the tests straight from the repository, without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
