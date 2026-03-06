"""
database.py – All PostgreSQL interactions via SQLAlchemy.
"""
import os
import json
import uuid
from functools import lru_cache
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv


# Load project-level .env when running app directly on host.
_PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))

# ── Connection ────────────────────────────────────────────────────
_db_user = os.getenv("POSTGRES_USER", "sentinel")
_db_password = os.getenv("POSTGRES_PASSWORD", "sentinel_pass")
_db_name = os.getenv("POSTGRES_DB", "sentineldb")
_db_host = os.getenv("POSTGRES_HOST", "localhost")
_db_port = os.getenv("POSTGRES_PORT", "5433")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    f"postgresql://{_db_user}:{_db_password}@{_db_host}:{_db_port}/{_db_name}"
)


def _ensure_schema(engine):
    schema_path = os.path.join(_PROJECT_ROOT, "streamlit_app", "schema.sql")
    if not os.path.exists(schema_path):
        return

    with open(schema_path, "r", encoding="utf-8") as f:
        sql_text = f.read()

    # Remove SQL comment lines and execute one statement at a time.
    cleaned_lines = []
    for line in sql_text.splitlines():
        stripped = line.strip()
        if stripped.startswith("--"):
            continue
        cleaned_lines.append(line)
    cleaned_sql = "\n".join(cleaned_lines)
    statements = [s.strip() for s in cleaned_sql.split(";") if s.strip()]

    with engine.begin() as conn:
        for stmt in statements:
            conn.execute(text(stmt))


@lru_cache(maxsize=1)
def get_engine():
    """Create one shared SQLAlchemy engine per process to reduce rerun latency."""
    engine = create_engine(DATABASE_URL, pool_pre_ping=True)
    _ensure_schema(engine)
    return engine


# ── Sessions ──────────────────────────────────────────────────────
def create_session(card_file: str, apm_file: str, card_rows: int, apm_rows: int) -> str:
    """Create a new upload session and return its UUID."""
    engine = get_engine()
    session_id = str(uuid.uuid4())
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO upload_sessions (session_id, card_file, apm_file, card_rows, apm_rows, status)
            VALUES (:sid, :cf, :af, :cr, :ar, 'processing')
        """), {"sid": session_id, "cf": card_file, "af": apm_file, "cr": card_rows, "ar": apm_rows})
    return session_id


def update_session(session_id: str, status: str, summary: dict):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE upload_sessions SET status=:s, summary=:sum WHERE session_id=:sid
        """), {"s": status, "sum": json.dumps(summary), "sid": session_id})


def get_all_sessions() -> pd.DataFrame:
    engine = get_engine()
    return pd.read_sql("""
        SELECT s.session_id, s.uploaded_at, s.card_file, s.apm_file,
               s.card_rows, s.apm_rows, s.status,
               s.summary->>'total_alerts'     AS total_alerts,
               s.summary->>'critical_alerts'  AS critical_alerts,
               s.summary->>'payout_risk_usd'  AS payout_risk_usd
        FROM upload_sessions s
        ORDER BY s.uploaded_at DESC
    """, engine)


def get_session(session_id: str) -> Optional[dict]:
    engine = get_engine()
    df = pd.read_sql(
        "SELECT * FROM upload_sessions WHERE session_id = %(sid)s",
        engine, params={"sid": session_id}
    )
    return df.to_dict("records")[0] if len(df) else None


# ── Card Masking Helper ───────────────────────────────────────────
def _mask_card(card_no: str) -> str:
    """Mask card number, keeping only last 4 digits for display."""
    if not card_no:
        return None
    card_str = str(card_no).strip()
    if len(card_str) <= 4:
        return card_str
    return f"****-****-****-{card_str[-4:]}"


# ── Transactions ──────────────────────────────────────────────────
def bulk_insert_transactions(session_id: str, df: pd.DataFrame, source: str):
    """Insert transactions, skipping duplicates."""
    if df is None or len(df) == 0:
        return
    engine = get_engine()
    rows = []
    for _, r in df.iterrows():
        card_raw = str(r.get("Card No", "")) if source == "CARD" else ""
        card_masked = _mask_card(card_raw) if card_raw else None
        rows.append({
            "session_id": session_id,
            "txid":       str(r.get("Txid", "")),
            "data_source": source,
            "email":      str(r.get("Email", ""))[:500] if pd.notna(r.get("Email")) else None,
            "card_no":    card_masked,
            "phone":      str(r.get("Phone", ""))[:100] if pd.notna(r.get("Phone")) else None,
            "amount":     float(r.get("Amount", 0) or 0),
            "amount_usd": float(r.get("Amount_USD", 0) or 0),
            "currency":   str(r.get("Currency", "USD"))[:10],
            "tx_status":  str(r.get("Status", ""))[:50],
            "tx_type":    str(r.get("Type", ""))[:100],
            "country":    str(r.get("Country", ""))[:100],
            "bin_country":str(r.get("BIN country", ""))[:100],
            "is_3d":      str(r.get("Is 3D", ""))[:10],
            "merchant":   str(r.get("Merchant", ""))[:255],
            "processor":  str(r.get("Processor", ""))[:255],
            "bank_name":  str(r.get("Bank name", ""))[:255],
            "error_desc": str(r.get("Error Description", ""))[:500] if pd.notna(r.get("Error Description")) else None,
            "tx_date":    _parse_date(r.get("Created Date (Server TZ)")),
        })
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO transactions
              (session_id,txid,data_source,email,card_no,phone,amount,amount_usd,
               currency,tx_status,tx_type,country,bin_country,is_3d,merchant,
               processor,bank_name,error_desc,tx_date)
            VALUES
              (:session_id,:txid,:data_source,:email,:card_no,:phone,:amount,:amount_usd,
               :currency,:tx_status,:tx_type,:country,:bin_country,:is_3d,:merchant,
               :processor,:bank_name,:error_desc,:tx_date)
            ON CONFLICT (txid, data_source) DO NOTHING
        """), rows)


def search_transactions(query: str, search_by: str) -> pd.DataFrame:
    engine = get_engine()
    col_map = {
        "Email":  "email",
        "Phone":  "phone",
        "Card No":"card_no",
        "Txid":   "txid",
    }
    col = col_map.get(search_by, "email")
    return pd.read_sql(f"""
        SELECT txid, data_source, email, card_no, phone,
               amount_usd, currency, tx_status, tx_type,
               country, merchant, is_3d, tx_date, session_id
        FROM transactions
        WHERE {col} ILIKE %(q)s
        ORDER BY tx_date DESC
        LIMIT 500
    """, engine, params={"q": f"%{query}%"})


def get_transaction_count() -> int:
    engine = get_engine()
    with engine.connect() as conn:
        r = conn.execute(text("SELECT COUNT(*) FROM transactions")).scalar()
    return r or 0


# ── Fraud Cases ───────────────────────────────────────────────────
def bulk_insert_cases(session_id: str, cases: list[dict]):
    """Insert list of fraud case dicts."""
    if not cases:
        return
    engine = get_engine()
    with engine.begin() as conn:
        for c in cases:
            seq_val = conn.execute(text("SELECT nextval('case_seq')")).scalar()
            case_ref = f"CASE-{seq_val:05d}"
            conn.execute(text("""
                INSERT INTO fraud_cases
                  (case_ref, session_id, alert_type, severity, entity_type, entity_value,
                   amount_usd, tx_count, details, status)
                VALUES
                  (:ref, :sid, :at, :sev, :et, :ev, :amt, :tc, :det, 'open')
                ON CONFLICT (case_ref) DO NOTHING
            """), {
                "ref": case_ref,
                "sid": session_id,
                "at":  c.get("alert_type", "unknown"),
                "sev": c.get("severity", "high"),
                "et":  c.get("entity_type", ""),
                "ev":  str(c.get("entity_value", ""))[:500],
                "amt": float(c.get("amount_usd", 0) or 0),
                "tc":  int(c.get("tx_count", 0) or 0),
                "det": json.dumps(c.get("details", {})),
            })


def get_cases(status: str = "all", session_id: str = None, exclude_whitelisted: bool = True) -> pd.DataFrame:
    engine = get_engine()
    where = []
    params = {}
    if status != "all":
        where.append("status = %(status)s")
        params["status"] = status
    if session_id:
        where.append("session_id = %(sid)s")
        params["sid"] = session_id
    
    # Exclude whitelisted entities
    if exclude_whitelisted:
        where.append("entity_value NOT IN (SELECT entity_value FROM trusted_entities)")
    
    clause = "WHERE " + " AND ".join(where) if where else ""
    return pd.read_sql(f"""
        SELECT case_ref, alert_type, severity, entity_type, entity_value,
               amount_usd, tx_count, status, notes, reviewed_at, created_at
        FROM fraud_cases {clause}
        ORDER BY
          CASE severity WHEN 'critical' THEN 1 WHEN 'high' THEN 2
                        WHEN 'medium' THEN 3 ELSE 4 END,
          amount_usd DESC
    """, engine, params=params)


def update_case_status(case_ref: str, status: str, notes: str, reviewer: str = "Risk Officer"):
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE fraud_cases
            SET status=:s, notes=:n, reviewed_by=:rb, reviewed_at=NOW()
            WHERE case_ref=:ref
        """), {"s": status, "n": notes, "rb": reviewer, "ref": case_ref})


def get_case_stats() -> dict:
    engine = get_engine()
    with engine.connect() as conn:
        r = conn.execute(text("""
            SELECT
              COUNT(*)                                       AS total,
              COUNT(*) FILTER (WHERE status='open')         AS open,
              COUNT(*) FILTER (WHERE status='confirmed_fraud') AS confirmed,
              COUNT(*) FILTER (WHERE status='false_positive')  AS false_pos,
              COUNT(*) FILTER (WHERE status='under_investigation') AS investigating,
              COUNT(*) FILTER (WHERE severity='critical')   AS critical,
              COALESCE(SUM(amount_usd) FILTER (WHERE severity IN ('critical','high')), 0) AS at_risk_usd
            FROM fraud_cases
        """)).fetchone()
    return dict(r._mapping) if r else {}


# ── Analysis Results ──────────────────────────────────────────────
def save_analysis_results(session_id: str, results: dict):
    def _json_safe(value):
        if isinstance(value, pd.DataFrame):
            df = value.copy()
            # Flatten non-JSON-safe column labels (e.g., MultiIndex tuples)
            df.columns = [
                " | ".join(map(str, c)) if isinstance(c, tuple) else str(c)
                for c in df.columns
            ]
            return _json_safe(df.to_dict(orient="records"))
        if isinstance(value, pd.Series):
            return _json_safe(value.to_dict())
        if isinstance(value, dict):
            return {str(k): _json_safe(v) for k, v in value.items()}
        if isinstance(value, (list, tuple)):
            return [_json_safe(v) for v in value]
        if isinstance(value, (datetime, pd.Timestamp)):
            return str(value)
        if pd.isna(value) if not isinstance(value, (str, bytes, dict, list, tuple)) else False:
            return None
        return value

    engine = get_engine()
    payload = _json_safe(results)
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO analysis_results (session_id, results)
            VALUES (:sid, :res)
            ON CONFLICT (session_id) DO UPDATE SET results=EXCLUDED.results
        """), {"sid": session_id, "res": json.dumps(payload, default=str)})


def get_analysis_results(session_id: str) -> dict:
    engine = get_engine()
    df = pd.read_sql(
        "SELECT results FROM analysis_results WHERE session_id = %(sid)s",
        engine, params={"sid": session_id}
    )
    if len(df):
        r = df.iloc[0]["results"]
        return r if isinstance(r, dict) else json.loads(r)
    return {}


# ── Whitelisting (Trusted Entities) ───────────────────────────────
def add_to_whitelist(entity_type: str, entity_value: str, reason: str = "", reviewer: str = "Risk Officer") -> bool:
    """Add an entity to the trusted whitelist."""
    engine = get_engine()
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO trusted_entities (entity_type, entity_value, reason, whitelisted_by)
                VALUES (:et, :ev, :r, :by)
                ON CONFLICT (entity_value) DO UPDATE SET reason=EXCLUDED.reason, whitelisted_at=NOW()
            """), {"et": entity_type, "ev": str(entity_value)[:500], "r": reason, "by": reviewer})
        return True
    except Exception as e:
        print(f"Whitelist add error: {e}")
        return False


def is_whitelisted(entity_value: str) -> bool:
    """Check if an entity is whitelisted."""
    engine = get_engine()
    with engine.connect() as conn:
        r = conn.execute(text(
            "SELECT COUNT(*) FROM trusted_entities WHERE entity_value = :ev"
        ), {"ev": str(entity_value)[:500]}).scalar()
    return r and r > 0


def get_whitelist() -> pd.DataFrame:
    """Get all whitelisted entities."""
    engine = get_engine()
    return pd.read_sql(
        "SELECT entity_type, entity_value, reason, whitelisted_by, whitelisted_at FROM trusted_entities ORDER BY whitelisted_at DESC",
        engine
    )


def remove_from_whitelist(entity_value: str):
    """Remove an entity from the whitelist."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(
            "DELETE FROM trusted_entities WHERE entity_value = :ev"
        ), {"ev": str(entity_value)[:500]})


        return c[:6] + "X" * (len(c) - 10) + c[-4:]
    return c


def _parse_date(val) -> Optional[datetime]:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return pd.to_datetime(val)
    except Exception:
        return None
