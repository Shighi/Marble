import sys
import os

# Add parent directory to path for page imports
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from .analyzer  import EnhancedFraudDetectionAnalyzer, BUILTIN_BLOCKED_COUNTRIES
from .database  import *
from .fx_rates  import get_fx_map, get_rate_info, convert_to_usd

# Optional report dependency (openpyxl). Keep core imports usable even when absent.
try:
    from .reports import build_excel_report
except Exception:
    build_excel_report = None
