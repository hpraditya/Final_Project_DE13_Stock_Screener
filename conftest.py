import sys
import os
from pathlib import Path
from dotenv import load_dotenv

# Load .env from project root so env vars are available for all tests.
load_dotenv(Path(__file__).parent / ".env")

# Make project root and airflow/dags importable from any test.
_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, _ROOT)
sys.path.insert(0, os.path.join(_ROOT, "airflow", "dags"))
