"""
written_report.py - Build a plain-text forensic report for copy/paste writeups.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any

import pandas as pd


def _to_df(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value
    if isinstance(value, list):
        return pd.DataFrame(value)
    if isinstance(value, dict):
        return pd.DataFrame([value])
    return pd.DataFrame()


def _top_lines(df: pd.DataFrame, label_col: str, value_col: str, prefix: str, n: int = 10) -> list[str]:
    if df is None or len(df) == 0 or label_col not in df.columns or value_col not in df.columns:
        return [f"  No {prefix.lower()} findings"]
    rows = df.sort_values(value_col, ascending=False).head(n)
    lines = []
    for _, r in rows.iterrows():
        lines.append(f"  {prefix} {r[label_col]}: {r[value_col]}")
    return lines


def build_written_report(results: dict, summary: dict, session_id: str | None = None) -> str:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines: list[str] = []
    lines.append("=" * 80)
    lines.append("EXECUTING COMPLETE ENHANCED FRAUD DETECTION ANALYSIS")
    lines.append("=" * 80)
    lines.append(f"Generated: {now}")
    if session_id:
        lines.append(f"Session ID: {session_id}")

    metadata = results.get("_metadata", {})
    date_range = metadata.get("date_range", {}) if isinstance(metadata, dict) else {}
    if date_range:
        lines.append(f"Date Coverage: {date_range}")
    lines.append("=" * 80)

    # BIN
    bin_r = results.get("enhanced_bin_analysis", {})
    bins_high = _to_df(bin_r.get("bins_highest"))
    bins_dec = _to_df(bin_r.get("bins_declines"))
    lines.append("")
    lines.append("ENHANCED BIN ANALYSIS")
    lines.append("-" * 60)
    lines.append(f"Total unique BINs: {bin_r.get('total_unique_bins', 0)}")
    lines.append("Top BINs by approved amounts:")
    lines.extend(_top_lines(bins_high, "BIN", "Approved_Amount_USD", "BIN"))
    if len(bins_dec):
        lines.append("Top BINs by declines:")
        lines.extend(_top_lines(bins_dec, "BIN", "Declined_Count", "BIN"))

    # Card
    card_r = results.get("enhanced_card_analysis", {})
    cards_high = _to_df(card_r.get("cards_highest"))
    cards_dec = _to_df(card_r.get("cards_declines"))
    lines.append("")
    lines.append("ENHANCED CARD ANALYSIS")
    lines.append("-" * 60)
    lines.append(f"Total unique cards: {card_r.get('total_cards', 0)}")
    lines.append("Top cards by approved amounts:")
    lines.extend(_top_lines(cards_high, "Card No", "Approved_Amount_USD", "Card"))
    if len(cards_dec):
        lines.append("Top cards by declines:")
        lines.extend(_top_lines(cards_dec, "Card No", "Declined_Count", "Card"))

    # Phone
    phone_r = results.get("enhanced_phone_analysis", {})
    phones_high = _to_df(phone_r.get("phones_highest"))
    phones_payout = _to_df(phone_r.get("phones_only_payouts"))
    lines.append("")
    lines.append("ENHANCED PHONE ANALYSIS")
    lines.append("-" * 60)
    lines.append(f"Total unique phones: {phone_r.get('total_unique_phones', 0)}")
    if len(phones_high):
        lines.append("Top phones by approved amounts:")
        lines.extend(_top_lines(phones_high, "Phone", "Approved_Amount_USD", "Phone"))
    lines.append(f"Payout-only phones detected: {len(phones_payout)}")
    if len(phones_payout):
        lines.append("Critical payout-only phones:")
        top_phone_cols = [c for c in ["Phone", "Payout_Amount", "Payout_Count"] if c in phones_payout.columns]
        for _, r in phones_payout[top_phone_cols].head(10).iterrows():
            lines.append(f"  Phone {r.get('Phone')}: ${float(r.get('Payout_Amount', 0)):,.2f} in {int(r.get('Payout_Count', 0))} payouts")

    # Email
    email_r = results.get("enhanced_email_analysis", {})
    high_email = _to_df(email_r.get("high_transacting"))
    payout_emails = _to_df(email_r.get("emails_only_payouts"))
    lines.append("")
    lines.append("ENHANCED EMAIL ANALYSIS")
    lines.append("-" * 60)
    lines.append(f"Total unique emails: {email_r.get('total_emails', 0)}")
    if len(high_email):
        # first metric column is tx count in current analyzer output
        metric_col = high_email.columns[0]
        lines.append("Top high transacting emails:")
        lines.extend(_top_lines(high_email.reset_index().rename(columns={"index": "Email"}), "Email", metric_col, "Email"))
    lines.append(f"Payout-only emails detected: {len(payout_emails)}")
    if len(payout_emails):
        for _, r in payout_emails.head(10).iterrows():
            lines.append(f"  Email {r.get('Email')}: ${float(r.get('Payout_Amount', 0)):,.2f} in {int(r.get('Payout_Count', 0))} payouts")

    # Cross networks
    cross = results.get("payout_only_cross_analysis", {})
    entities = _to_df(cross.get("payout_only_entities"))
    networks = _to_df(cross.get("fraud_networks"))
    lines.append("")
    lines.append("PAYOUT-ONLY FRAUD NETWORK DETECTION")
    lines.append("-" * 60)
    lines.append(f"Total payout-only entities: {len(entities)}")
    if len(entities) and "Payout_Amount" in entities.columns:
        lines.append(f"Total payout amount (USD): ${float(entities['Payout_Amount'].sum()):,.2f}")
    lines.append(f"Potential fraud networks detected: {len(networks)}")
    if len(networks):
        for _, r in networks.head(10).iterrows():
            lines.append(
                f"  CRITICAL: {r.get('Entity1_Type')} {r.get('Entity1')} <-> "
                f"{r.get('Entity2_Type')} {r.get('Entity2')} | "
                f"${float(r.get('Total_Payout_Amount', 0)):,.2f}"
            )

    # Velocity
    vel = results.get("velocity_violations", {})
    daily_v = _to_df(vel.get("daily_violations")).reset_index()
    hourly_v = _to_df(vel.get("hourly_violations")).reset_index()
    lines.append("")
    lines.append("VELOCITY RULE ANALYSIS")
    lines.append("-" * 60)
    lines.append(f"Cards exceeding daily limit: {len(daily_v)}")
    lines.append(f"Cards exceeding hourly limit: {len(hourly_v)}")

    # Merchant + sanctions + 3DS
    m = results.get("merchant_analysis", {})
    risky_merchants = _to_df(m.get("risky_merchants"))
    blocked = results.get("blocked_countries", {})
    blocked_count = blocked.get("blocked_count", 0)
    s3d = results.get("secure_3d_analysis", {})
    high_non3d = _to_df(s3d.get("high_value_non_3d"))
    lines.append("")
    lines.append("MERCHANT / SANCTIONS / 3DS")
    lines.append("-" * 60)
    lines.append(f"Risky merchants: {len(risky_merchants)}")
    lines.append(f"Blocked-country transactions: {int(blocked_count or 0)}")
    lines.append(f"High-value non-3D transactions: {len(high_non3d)}")

    # Final summary
    lines.append("")
    lines.append("=" * 80)
    lines.append("COMPLETE ANALYSIS SUMMARY")
    lines.append("=" * 80)
    lines.append(f"Total alerts: {summary.get('total_alerts', 0)}")
    lines.append(f"Critical alerts: {summary.get('critical_alerts', 0)}")
    lines.append(f"Payout-only phones: {summary.get('payout_phones', 0)}")
    lines.append(f"Payout-only emails: {summary.get('payout_emails', 0)}")
    lines.append(f"Velocity violations: daily={summary.get('velocity_daily', 0)}, hourly={summary.get('velocity_hourly', 0)}")
    lines.append(f"Fraud networks: {summary.get('fraud_networks', 0)}")
    lines.append(f"Total at-risk payout amount (USD): ${float(summary.get('payout_risk_usd', 0) or 0):,.2f}")
    lines.append("=" * 80)
    lines.append("FRAUD DETECTION ANALYSIS COMPLETE")
    lines.append("=" * 80)
    return "\n".join(lines)
