"""
analyzer.py – EnhancedFraudDetectionAnalyzer adapted for Sentinel.
Returns structured results instead of printing; all rule logic is preserved.
"""
import io
import warnings
import pandas as pd
import numpy as np
from datetime import datetime

warnings.filterwarnings("ignore")

# ── Live FX rates (falls back to hardcoded if network unavailable) ─
from .fx_rates import get_fx_map

# ── Built-in sanctioned / blocked countries list ─────────────────
# This list is used as the default when no BLOCKED COUNTRIES CSV is uploaded.
# It is also always merged with any uploaded CSV so the CSV only needs to
# contain *additions* rather than a full replacement.
BUILTIN_BLOCKED_COUNTRIES: list[str] = [
    "AFGHANISTAN",
    "BELARUS",
    "BURMA",
    "BURUNDI",
    "CENTRAL AFRICAN REPUBLIC",
    "CRIMEA",
    "CUBA",
    "DEMOCRATIC PEOPLE'S REPUBLIC OF KOREA",
    "NORTH KOREA",
    "DEMOCRATIC REPUBLIC OF CONGO",
    "DEMOCRATIC REPUBLIC OF THE CONGO",
    "DRC",
    "DONETSK PEOPLE'S REPUBLIC",
    "DNR",
    "EQUATORIAL GUINEA",
    "GUINEA-BISSAU",
    "HAITI",
    "IRAN",
    "IRAQ",
    "KOSOVO",
    "LEBANON",
    "LIBYA",
    "LUHANSK PEOPLE'S REPUBLIC",
    "LNR",
    "MYANMAR",
    "NICARAGUA",
    "PALESTINIAN AUTHORITY",
    "GAZA",
    "WEST BANK",
    "RUSSIA",
    "SOMALIA",
    "SOUTH SUDAN",
    "SUDAN",
    "SYRIA",
    "UKRAINE",
    "UNITED STATES",
    "UNITED STATES MINOR OUTLYING ISLANDS",
    "US MINOR OUTLYING ISLANDS",
    "VENEZUELA",
    "YEMEN",
    "ZAPORIZHZHIA REGION",
    "ZAPORIZHZHIA",
    "ZIMBABWE",
]


class EnhancedFraudDetectionAnalyzer:

    def __init__(self):
        self.card_df       = None
        self.apm_df        = None
        self.blocked_df    = None
        self.combined_df   = None
        self.results       = {}
        self.log           = []   # list of (level, message) tuples
        self.date_range    = {}   # {source: {"start": datetime, "end": datetime}}

    # ── Logging helper ────────────────────────────────────────────
    def _log(self, level: str, msg: str):
        self.log.append((level, msg))

    # ── Data loading ──────────────────────────────────────────────
    def load_from_dataframes(self, card_df: pd.DataFrame = None,
                              apm_df: pd.DataFrame = None,
                              blocked_df: pd.DataFrame = None):
        """Load data from pre-read DataFrames (no file I/O)."""
        self._log("info", "Loading data from uploaded files…")

        if card_df is not None and len(card_df):
            self.card_df = self._convert_to_usd(card_df.copy())
            self.card_df = self._extract_bin(self.card_df)
            self.card_df["Created Date (Server TZ)"] = pd.to_datetime(
                self.card_df.get("Created Date (Server TZ)", pd.Series(dtype="object")),
                errors="coerce"
            )
            # Extract date range
            valid_dates = self.card_df["Created Date (Server TZ)"].dropna()
            if len(valid_dates) > 0:
                self.date_range["Card"] = {
                    "start": str(valid_dates.min().date()),
                    "end": str(valid_dates.max().date()),
                }
            self._log("success", f"Card TM loaded: {self.card_df.shape}")

        if apm_df is not None and len(apm_df):
            self.apm_df = self._convert_to_usd(apm_df.copy())
            self.apm_df["Created Date (Server TZ)"] = pd.to_datetime(
                self.apm_df.get("Created Date (Server TZ)", pd.Series(dtype="object")),
                errors="coerce"
            )
            # Extract date range
            valid_dates = self.apm_df["Created Date (Server TZ)"].dropna()
            if len(valid_dates) > 0:
                self.date_range["APM"] = {
                    "start": str(valid_dates.min().date()),
                    "end": str(valid_dates.max().date()),
                }
            self._log("success", f"APM TM loaded: {self.apm_df.shape}")

        if blocked_df is not None and len(blocked_df):
            self.blocked_df = blocked_df.copy()
            self._log("success", f"Blocked countries loaded: {len(self.blocked_df)}")

        # Build combined
        sources = []
        if self.card_df is not None:
            tmp = self.card_df.copy(); tmp["Data_Source"] = "Card"; sources.append(tmp)
        if self.apm_df is not None:
            tmp = self.apm_df.copy(); tmp["Data_Source"] = "APM"; sources.append(tmp)
        if len(sources) == 2:
            common = list(set(sources[0].columns) & set(sources[1].columns))
            if len(common) > 3:
                self.combined_df = pd.concat(
                    [s[common] for s in sources], ignore_index=True
                )
        elif len(sources) == 1:
            self.combined_df = sources[0]

    # ── Currency conversion ───────────────────────────────────────
    def _convert_to_usd(self, df: pd.DataFrame) -> pd.DataFrame:
        if "Amount" not in df.columns or "Currency" not in df.columns:
            return df
        df   = df.copy()
        fx   = get_fx_map()   # live rates, cached for 4 h
        df["Original_Amount"]   = df["Amount"]
        df["Original_Currency"] = df["Currency"]
        def _conv(row):
            try:
                return float(row["Amount"]) * fx.get(str(row["Currency"]).upper(), 1.0)
            except Exception:
                return row["Amount"]
        df["Amount_USD"] = df.apply(_conv, axis=1)
        df["Amount"]     = df["Amount_USD"]
        return df

    def _extract_bin(self, df: pd.DataFrame) -> pd.DataFrame:
        if "Card No" not in df.columns:
            return df
        df = df.copy()
        df["BIN"] = df["Card No"].astype(str).str[:6]
        return df

    # ── Run all analyses ──────────────────────────────────────────
    def run_all(self):
        analyses = [
            ("enhanced_bin_analysis",       self._enhanced_bin_analysis),
            ("enhanced_card_analysis",      self._enhanced_card_analysis),
            ("enhanced_phone_analysis",     self._enhanced_phone_analysis),
            ("enhanced_email_analysis",     self._enhanced_email_analysis),
            ("payout_only_cross_analysis",  self._payout_only_cross_analysis),
            ("recurring_card_patterns",     self._recurring_card_patterns),
            ("velocity_violations",         self._velocity_rule_analysis),
            ("suspicious_timing",           self._suspicious_timing_analysis),
            ("merchant_trends",             self._merchant_trend_analysis),
            ("merchant_analysis",           self._merchant_analysis),
            ("blocked_countries",           self._blocked_countries_analysis),
            ("secure_3d_analysis",          self._secure_3d_analysis),
        ]
        for key, fn in analyses:
            try:
                fn()
            except Exception as e:
                self._log("error", f"{key} failed: {e}")
                self.results[key] = {}
        
        # Add metadata to results
        self.results["_metadata"] = {
            "date_range": self.date_range,
            "analysis_timestamp": str(pd.Timestamp.now()),
            "total_card_rows": len(self.card_df) if self.card_df is not None else 0,
            "total_apm_rows": len(self.apm_df) if self.apm_df is not None else 0,
        }
        
        return self.results

    # ── Build fraud cases list for DB ─────────────────────────────
    def build_cases(self) -> list[dict]:
        cases = []
        
        # Helper function for severity scoring
        def score_severity(amount_usd: float, tx_count: int, is_payout_only: bool = False) -> str:
            """Intelligent severity scoring to reduce alert fatigue."""
            if amount_usd >= 1000:
                return "critical"
            elif amount_usd >= 500 or (is_payout_only and amount_usd >= 100):
                return "high"
            elif amount_usd >= 100 or tx_count >= 10:
                return "medium"
            else:
                return "low"

        # Payout-only phones (MIN $100 threshold to reduce noise)
        r = self.results.get("enhanced_phone_analysis", {})
        phones_df = r.get("phones_only_payouts", pd.DataFrame())
        if phones_df is not None and len(phones_df):
            for _, row in phones_df.iterrows():
                amount = float(row.get("Payout_Amount", 0) or 0)
                # Filter: only include if amount >= $100
                if amount < 100:
                    continue
                    
                cases.append({
                    "alert_type":   "payout_only",
                    "severity":     score_severity(amount, int(row.get("Payout_Count", 0) or 0), is_payout_only=True),
                    "entity_type":  "phone",
                    "entity_value": str(row.get("Phone", "")),
                    "amount_usd":   amount,
                    "tx_count":     int(row.get("Payout_Count", 0) or 0),
                    "details": {
                        "emails": int(row.get("Total_Emails_Used", 0) or 0),
                        "period": f"{row.get('First_Payout','')} → {row.get('Last_Payout','')}",
                    },
                })

        # Payout-only emails (MIN $100 threshold to reduce noise)
        r2 = self.results.get("enhanced_email_analysis", {})
        emails_df = r2.get("emails_only_payouts", pd.DataFrame())
        if emails_df is not None and len(emails_df):
            for _, row in emails_df.iterrows():
                amount = float(row.get("Payout_Amount", 0) or 0)
                # Filter: only include if amount >= $100
                if amount < 100:
                    continue
                    
                cases.append({
                    "alert_type":   "payout_only",
                    "severity":     score_severity(amount, int(row.get("Payout_Count", 0) or 0), is_payout_only=True),
                    "entity_type":  "email",
                    "entity_value": str(row.get("Email", "")),
                    "amount_usd":   amount,
                    "tx_count":     int(row.get("Payout_Count", 0) or 0),
                    "details": {
                        "cards_used": int(row.get("Total_Cards_Used", 0) or 0),
                        "phones_used": int(row.get("Total_Phones_Used", 0) or 0),
                    },
                })

        # Velocity violations — daily
        rv = self.results.get("velocity_violations", {})
        dv = rv.get("daily_violations", pd.DataFrame())
        if dv is not None and len(dv):
            for card, row in dv.iterrows():
                cases.append({
                    "alert_type":   "velocity",
                    "severity":     "medium",
                    "entity_type":  "card",
                    "entity_value": str(card),
                    "amount_usd":   0,
                    "tx_count":     int(row.get("max", 0) or 0),
                    "details": {"violation_days": int(row.get("count", 0) or 0)},
                })

        # Blocked countries - ALWAYS CRITICAL
        rb = self.results.get("blocked_countries", {})
        bt = rb.get("blocked_transactions", pd.DataFrame())
        if bt is not None and len(bt):
            for _, row in bt.iterrows():
                cases.append({
                    "alert_type":   "sanctions",
                    "severity":     "critical",  # Sanctions are always critical
                    "entity_type":  "card",
                    "entity_value": str(row.get("Card No", row.get("Txid", ""))),
                    "amount_usd":   float(row.get("Amount_USD", row.get("Amount", 0)) or 0),
                    "tx_count":     1,
                    "details": {"country": str(row.get("BIN country", row.get("Country", "")))},
                })

        # 3DS anomaly – high value non-3D
        r3 = self.results.get("secure_3d_analysis", {})
        high_non3d = r3.get("high_value_non_3d", pd.DataFrame())
        if high_non3d is not None and len(high_non3d):
            for _, row in high_non3d.iterrows():
                amount = float(row.get("Amount", 0) or 0)
                # Only include high-value non-3D ($500+)
                if amount < 500:
                    continue
                cases.append({
                    "alert_type":   "3ds_anomaly",
                    "severity":     score_severity(amount, 1, is_payout_only=False),
                    "entity_type":  "card",
                    "entity_value": str(row.get("Card No", row.get("Txid", ""))),
                    "amount_usd":   amount,
                    "tx_count":     1,
                    "details": {"merchant": str(row.get("Merchant", "")), "txid": str(row.get("Txid", ""))},
                })

        # Suspicious recurring card patterns
        rrp = self.results.get("recurring_card_patterns", {})
        susp = rrp.get("suspicious_recurring", pd.DataFrame())
        if susp is not None and len(susp):
            for _, row in susp.iterrows():
                cases.append({
                    "alert_type":   "recurring",
                    "severity":     "medium",
                    "entity_type":  "card",
                    "entity_value": str(row.get("Card_No", "")),
                    "amount_usd":   0,
                    "tx_count":     int(row.get("Transaction_Count", 0) or 0),
                    "details": {"quick_tx": int(row.get("Quick_Transactions", 0) or 0)},
                })

        return cases

    # ── Individual analysis methods ───────────────────────────────
    def _enhanced_bin_analysis(self):
        if self.card_df is None:
            return
        df = self.card_df
        aggs = {"Status": ["count", lambda x: (x=="approved").sum(), lambda x: (x=="declined").sum()]}
        if "Amount" in df.columns:
            aggs["Amount"] = ["count", "sum", "mean"]
        bin_stats = df.groupby("BIN").agg(aggs).round(2)

        approved_data = df[df["Status"]=="approved"].groupby("BIN")["Amount"].sum().reset_index()
        approved_data.columns = ["BIN", "Approved_Amount_USD"]
        bins_highest = approved_data.sort_values("Approved_Amount_USD", ascending=False).head(10)
        bins_lowest  = approved_data[approved_data["Approved_Amount_USD"]>0].sort_values("Approved_Amount_USD").head(10)

        declined_data = df[df["Status"]=="declined"]
        bin_errors = pd.DataFrame()
        if len(declined_data) and "Error Description" in declined_data.columns:
            bin_errors = declined_data.groupby(["BIN","Error Description"]).size().reset_index(name="Error_Count")
            bin_errors = bin_errors.sort_values("Error_Count", ascending=False).head(20)

        bins_declines = df.groupby("BIN").apply(lambda x: (x["Status"]=="declined").sum()).reset_index()
        bins_declines.columns = ["BIN","Declined_Count"]

        self.results["enhanced_bin_analysis"] = {
            "bin_stats":          bin_stats,
            "bins_highest":       bins_highest,
            "bins_lowest":        bins_lowest,
            "bins_declines":      bins_declines.sort_values("Declined_Count", ascending=False).head(10),
            "bin_errors":         bin_errors,
            "total_unique_bins":  df["BIN"].nunique(),
        }
        self._log("success", f"BIN analysis: {df['BIN'].nunique()} unique BINs")

    def _enhanced_card_analysis(self):
        if self.card_df is None or "Card No" not in self.card_df.columns:
            return
        df = self.card_df
        card_stats = df.groupby("Card No").agg({
            "Amount":  ["count","sum","mean"] if "Amount" in df.columns else ["count"],
            "Status":  ["count", lambda x: (x=="approved").sum(), lambda x: (x=="declined").sum()],
            "Email":   "nunique" if "Email" in df.columns else "count",
        }).round(2)

        approved = df[df["Status"]=="approved"].groupby("Card No")["Amount"].sum().reset_index()
        approved.columns = ["Card No","Approved_Amount_USD"]
        cards_highest = approved.sort_values("Approved_Amount_USD", ascending=False).head(10)

        declined_counts = df.groupby("Card No").apply(lambda x: (x["Status"]=="declined").sum()).reset_index()
        declined_counts.columns = ["Card No","Declined_Count"]
        cards_declines = declined_counts.sort_values("Declined_Count", ascending=False).head(10)

        self.results["enhanced_card_analysis"] = {
            "card_stats":      card_stats,
            "cards_highest":   cards_highest,
            "cards_declines":  cards_declines,
            "total_cards":     df["Card No"].nunique(),
        }

    def _enhanced_phone_analysis(self):
        if self.apm_df is None or "Phone" not in self.apm_df.columns:
            return
        df = self.apm_df
        phone_stats = df.groupby("Phone").agg({
            "Amount": ["count","sum","mean"] if "Amount" in df.columns else ["count"],
            "Status": ["count", lambda x: (x=="approved").sum(), lambda x: (x=="declined").sum()],
            "Email":  "nunique" if "Email" in df.columns else "count",
        }).round(2)

        phones_highest = pd.DataFrame()
        if "Amount" in df.columns and "Status" in df.columns:
            approved = df[df["Status"]=="approved"].groupby("Phone")["Amount"].sum().reset_index()
            approved.columns = ["Phone","Approved_Amount_USD"]
            phones_highest = approved.sort_values("Approved_Amount_USD", ascending=False).head(10)

        # Payout-only (major fraud indicator)
        payout_only, type_analysis = [], []
        if "Type" in df.columns:
            approved_data = df[df["Status"]=="approved"] if "Status" in df.columns else df
            payouts = approved_data[approved_data["Type"].str.contains("payout|withdrawal|refund", case=False, na=False)]
            payins  = approved_data[approved_data["Type"].str.contains("sale|payment|deposit", case=False, na=False)]
            for phone in payouts["Phone"].unique():
                if pd.isna(phone): continue
                phone_payins = payins[payins["Phone"]==phone]
                if len(phone_payins) == 0:
                    phone_payouts = payouts[payouts["Phone"]==phone]
                    payout_only.append({
                        "Phone": phone,
                        "Payout_Amount": phone_payouts["Amount"].sum() if "Amount" in phone_payouts.columns else 0,
                        "Payout_Count":  len(phone_payouts),
                        "Total_Emails_Used": phone_payouts["Email"].nunique() if "Email" in phone_payouts.columns else 0,
                        "First_Payout": phone_payouts["Created Date (Server TZ)"].min() if "Created Date (Server TZ)" in phone_payouts.columns else None,
                        "Last_Payout":  phone_payouts["Created Date (Server TZ)"].max() if "Created Date (Server TZ)" in phone_payouts.columns else None,
                    })
                payout_amt  = payouts[payouts["Phone"]==phone]["Amount"].sum() if "Amount" in df.columns else 0
                payin_amt   = phone_payins["Amount"].sum() if "Amount" in df.columns else 0
                if payout_amt > payin_amt and payout_amt > 0:
                    type_analysis.append({
                        "Phone": phone,
                        "Payout_Amount": payout_amt,
                        "Payin_Amount":  payin_amt,
                        "Net_Payout":    payout_amt - payin_amt,
                    })

        phones_only_payouts = pd.DataFrame(payout_only).sort_values("Payout_Amount", ascending=False) if payout_only else pd.DataFrame()
        payout_payin        = pd.DataFrame(type_analysis).sort_values("Net_Payout", ascending=False) if type_analysis else pd.DataFrame()

        self.results["enhanced_phone_analysis"] = {
            "phone_stats":         phone_stats,
            "phones_highest":      phones_highest,
            "phones_only_payouts": phones_only_payouts,
            "payout_payin":        payout_payin,
            "total_phones":        df["Phone"].nunique(),
        }
        self._log("success", f"Phone analysis: {len(phones_only_payouts)} payout-only phones")

    def _enhanced_email_analysis(self):
        sources = []
        if self.card_df is not None and "Email" in self.card_df.columns:
            tmp = self.card_df.copy(); tmp["Data_Source"] = "Card"; sources.append(tmp)
        if self.apm_df is not None and "Email" in self.apm_df.columns:
            tmp = self.apm_df.copy(); tmp["Data_Source"] = "APM"; sources.append(tmp)
        if not sources:
            return

        if len(sources) == 2:
            common = list(set(sources[0].columns) & set(sources[1].columns))
            data = pd.concat([s[common] for s in sources], ignore_index=True) if len(common)>3 else sources[0]
        else:
            data = sources[0]

        agg_d = {"Amount": ["count","sum","mean"] if "Amount" in data.columns else ["count"],
                 "Status": ["count", lambda x: (x=="approved").sum(), lambda x: (x=="declined").sum()]}
        if "Card No" in data.columns:
            agg_d["Card No"] = "nunique"
        if "Phone" in data.columns:
            agg_d["Phone"] = "nunique"
        email_stats = data.groupby("Email").agg(agg_d).round(2)

        high_trans = email_stats.nlargest(10, email_stats.columns[0]) if len(email_stats) else pd.DataFrame()

        # Emails with more than 3 cards
        multi_card = pd.DataFrame()
        if "Card No" in data.columns:
            mc = data.groupby("Email")["Card No"].nunique().reset_index()
            mc.columns = ["Email","Unique_Cards"]
            multi_card = mc[mc["Unique_Cards"]>3].sort_values("Unique_Cards", ascending=False)

        # Payout-only emails
        payout_only = []
        if "Type" in data.columns:
            approved = data[data["Status"]=="approved"] if "Status" in data.columns else data
            payouts = approved[approved["Type"].str.contains("payout|withdrawal|refund", case=False, na=False)]
            payins  = approved[approved["Type"].str.contains("sale|payment|deposit",   case=False, na=False)]
            for email in payouts["Email"].dropna().unique():
                email_payins = payins[payins["Email"]==email]
                if len(email_payins) == 0:
                    ep = payouts[payouts["Email"]==email]
                    payout_only.append({
                        "Email":          email,
                        "Payout_Amount":  ep["Amount"].sum() if "Amount" in ep.columns else 0,
                        "Payout_Count":   len(ep),
                        "Total_Cards_Used":  ep["Card No"].nunique() if "Card No" in ep.columns else 0,
                        "Total_Phones_Used": ep["Phone"].nunique() if "Phone" in ep.columns else 0,
                    })

        emails_only_payouts = pd.DataFrame(payout_only).sort_values("Payout_Amount", ascending=False) if payout_only else pd.DataFrame()

        self.results["enhanced_email_analysis"] = {
            "email_stats":        email_stats,
            "high_transacting":   high_trans,
            "emails_multiple_cards": multi_card,
            "emails_only_payouts": emails_only_payouts,
            "total_emails":       data["Email"].nunique(),
        }
        self._log("success", f"Email analysis: {len(emails_only_payouts)} payout-only emails")

    def _payout_only_cross_analysis(self):
        if self.apm_df is None:
            return
        df = self.apm_df
        if "Type" not in df.columns:
            return
        approved = df[df["Status"]=="approved"] if "Status" in df.columns else df
        payouts = approved[approved["Type"].str.contains("payout|withdrawal|refund", case=False, na=False)]
        payins  = approved[approved["Type"].str.contains("sale|payment|deposit",   case=False, na=False)]

        payout_only_entities = []
        for col in ["Email","Phone"]:
            if col not in df.columns:
                continue
            for entity in payouts[col].dropna().unique():
                entity_payins = payins[payins[col]==entity]
                if len(entity_payins) == 0:
                    ep = payouts[payouts[col]==entity]
                    payout_only_entities.append({
                        "Entity_Type":  col,
                        "Entity":       entity,
                        "Payout_Amount": ep["Amount"].sum() if "Amount" in ep.columns else 0,
                        "Payout_Count": len(ep),
                    })

        payout_df = pd.DataFrame(payout_only_entities).sort_values("Payout_Amount", ascending=False) if payout_only_entities else pd.DataFrame()

        # Fraud networks
        fraud_networks = []
        if len(payout_df) > 1 and "Email" in df.columns and "Phone" in df.columns:
            phone_ent = payout_df[payout_df["Entity_Type"]=="Phone"]
            email_ent = payout_df[payout_df["Entity_Type"]=="Email"]
            for _, pe in phone_ent.iterrows():
                phone = pe["Entity"]
                phone_emails = set(payouts[payouts["Phone"]==phone]["Email"].dropna())
                for em in phone_emails:
                    if em in email_ent["Entity"].values:
                        ee = email_ent[email_ent["Entity"]==em].iloc[0]
                        fraud_networks.append({
                            "Entity1_Type": "Phone", "Entity1": phone,
                            "Entity2_Type": "Email", "Entity2": em,
                            "Total_Payout_Amount": pe["Payout_Amount"] + ee["Payout_Amount"],
                            "Risk_Level": "CRITICAL",
                        })

        fraud_df = pd.DataFrame(fraud_networks).sort_values("Total_Payout_Amount", ascending=False) if fraud_networks else pd.DataFrame()

        self.results["payout_only_cross_analysis"] = {
            "payout_only_entities": payout_df,
            "fraud_networks":       fraud_df,
            "summary": {
                "total_entities":        len(payout_df),
                "total_payout_amount":   float(payout_df["Payout_Amount"].sum()) if len(payout_df) else 0,
                "total_networks":        len(fraud_df),
            },
        }
        self._log("success", f"Cross-analysis: {len(payout_df)} payout-only entities, {len(fraud_df)} networks")

    def _recurring_card_patterns(self):
        if self.card_df is None or "Card No" not in self.card_df.columns:
            return
        df = self.card_df
        freq = df["Card No"].value_counts()
        recurring = freq[freq > 5].index[:20]

        details = []
        for card in recurring:
            cd = df[df["Card No"]==card]
            avg_int = min_int = very_quick = same_time = 0
            if "Created Date (Server TZ)" in cd.columns:
                cd_sorted = cd.sort_values("Created Date (Server TZ)")
                dates = pd.to_datetime(cd_sorted["Created Date (Server TZ)"])
                diffs = dates.diff().dt.total_seconds() / 60
                avg_int   = diffs.mean()
                min_int   = diffs.min()
                very_quick = (diffs <= 1).sum()
                same_time = cd.groupby(cd["Created Date (Server TZ)"].dt.floor("h") if hasattr(cd["Created Date (Server TZ)"].dt,"floor") else "Created Date (Server TZ)").size().max()
            details.append({
                "Card_No":          card,
                "Transaction_Count": len(cd),
                "Unique_Emails":    cd["Email"].nunique() if "Email" in cd.columns else 0,
                "Total_Amount":     cd["Amount"].sum() if "Amount" in cd.columns else 0,
                "Approved_Count":   (cd["Status"]=="approved").sum() if "Status" in cd.columns else 0,
                "Avg_Interval_Minutes": avg_int,
                "Min_Interval_Minutes": min_int,
                "Quick_Transactions": very_quick,
                "Max_Hourly_Transactions": same_time,
            })

        recurring_df = pd.DataFrame(details) if details else pd.DataFrame()
        suspicious = pd.DataFrame()
        if len(recurring_df):
            suspicious = recurring_df[
                (recurring_df["Quick_Transactions"] > 0) |
                (recurring_df["Max_Hourly_Transactions"] > 10) |
                (recurring_df["Unique_Emails"] > 2)
            ].sort_values("Transaction_Count", ascending=False)

        self.results["recurring_card_patterns"] = {
            "recurring_df":       recurring_df,
            "suspicious_recurring": suspicious,
        }

    def _velocity_rule_analysis(self, daily_limit=10, hourly_limit=5):
        if self.card_df is None or "Card No" not in self.card_df.columns:
            return
        if "Created Date (Server TZ)" not in self.card_df.columns:
            return
        vd = self.card_df.copy()
        vd["Date"] = pd.to_datetime(vd["Created Date (Server TZ)"]).dt.date
        vd["Hour"] = pd.to_datetime(vd["Created Date (Server TZ)"]).dt.floor("h")

        daily  = vd.groupby(["Card No","Date"]).size()
        hourly = vd.groupby(["Card No","Hour"]).size()
        daily_v  = daily[daily > daily_limit]
        hourly_v = hourly[hourly > hourly_limit]

        cards_daily  = daily_v.groupby("Card No").agg(["count","max","mean"]).round(2) if len(daily_v) else pd.DataFrame()
        cards_hourly = hourly_v.groupby("Card No").agg(["count","max","mean"]).round(2) if len(hourly_v) else pd.DataFrame()

        self.results["velocity_violations"] = {
            "daily_violations":  cards_daily,
            "hourly_violations": cards_hourly,
        }
        self._log("success", f"Velocity: {len(cards_daily)} daily, {len(cards_hourly)} hourly violators")

    def _suspicious_timing_analysis(self):
        datasets = []
        if self.card_df is not None and "Created Date (Server TZ)" in self.card_df.columns:
            t = self.card_df.copy(); t["Data_Source"]="Card"; datasets.append(t)
        if self.apm_df is not None and "Created Date (Server TZ)" in self.apm_df.columns:
            t = self.apm_df.copy(); t["Data_Source"]="APM"; datasets.append(t)
        if not datasets:
            return

        if len(datasets)==2:
            common = list(set(datasets[0].columns)&set(datasets[1].columns))
            data = pd.concat([d[common] for d in datasets], ignore_index=True) if len(common)>5 else datasets[0]
        else:
            data = datasets[0]

        data["DateTime"]   = pd.to_datetime(data["Created Date (Server TZ)"])
        data["Hour"]       = data["DateTime"].dt.hour
        data["HourMinute"] = data["DateTime"].dt.strftime("%H:%M")
        data["DayOfWeek"]  = data["DateTime"].dt.dayofweek

        hm_counts     = data["HourMinute"].value_counts()
        suspicious_times = hm_counts[hm_counts > 20]

        # Daily time patterns
        daily_patterns = []
        for col in ["Email","Phone","Card No"]:
            if col not in data.columns: continue
            for entity in data[col].dropna().unique():
                ed = data[data[col]==entity]
                if len(ed) < 5: continue
                hc = ed["Hour"].value_counts()
                if hc.max() > len(ed)*0.7:
                    daily_patterns.append({
                        "Entity_Type": col, "Entity": entity,
                        "Dominant_Hour": hc.idxmax(),
                        "Percentage": round(hc.max()/len(ed)*100,2),
                        "Total_Transactions": len(ed),
                    })

        daily_df = pd.DataFrame(daily_patterns).sort_values("Percentage", ascending=False) if daily_patterns else pd.DataFrame()

        self.results["suspicious_timing"] = {
            "suspicious_times": suspicious_times,
            "daily_patterns":   daily_df,
        }

    def _merchant_trend_analysis(self):
        src = None
        for d in [self.card_df, self.apm_df, self.combined_df]:
            if d is not None and "Merchant" in d.columns and "Created Date (Server TZ)" in d.columns:
                src = d; break
        if src is None:
            return

        approved = src[src["Status"]=="approved"].copy() if "Status" in src.columns else src.copy()
        if len(approved)==0:
            return
        approved["Week"] = pd.to_datetime(approved["Created Date (Server TZ)"]).dt.to_period("W")
        weekly = approved.groupby(["Merchant","Week"]).agg(
            {"Amount": ["count","sum"]} if "Amount" in approved.columns else {"Txid": "count"}
        ).round(2)

        trends = []
        for merchant in approved["Merchant"].unique():
            mw = weekly.loc[merchant] if merchant in weekly.index else pd.DataFrame()
            if len(mw) < 3: continue
            first, last = mw.iloc[0], mw.iloc[-1]
            change = ((last.iloc[0]-first.iloc[0])/max(first.iloc[0],1))*100
            trends.append({
                "Merchant": merchant,
                "Count_Change_Percent": round(change,1),
                "Trend": "Increasing" if change>50 else "Decreasing" if change<-50 else "Stable",
            })

        trends_df = pd.DataFrame(trends) if trends else pd.DataFrame()
        self.results["merchant_trends"] = {
            "trends_df":          trends_df,
            "increasing":         trends_df[trends_df["Trend"]=="Increasing"] if len(trends_df) else pd.DataFrame(),
            "decreasing":         trends_df[trends_df["Trend"]=="Decreasing"] if len(trends_df) else pd.DataFrame(),
        }

    def _merchant_analysis(self):
        src = None
        for d, name in [(self.card_df,"Card TM"),(self.apm_df,"APM TM"),(self.combined_df,"Combined")]:
            if d is not None and "Merchant" in d.columns and "Status" in d.columns:
                src = d; break
        if src is None:
            return

        stats = src.groupby("Merchant").agg({
            "Status": ["count", lambda x: (x=="approved").sum()],
            "Amount": "sum" if "Amount" in src.columns else "count",
        }).round(2)
        stats.columns = ["Total_Transactions","Approved_Count","Total_Amount_USD"]
        stats["Approval_Ratio"] = (stats["Approved_Count"]/stats["Total_Transactions"]*100).round(2)
        stats["Declined_Count"] = stats["Total_Transactions"]-stats["Approved_Count"]

        risky = stats[(stats["Approval_Ratio"]<30)|(stats["Approval_Ratio"]>95)].sort_values("Approval_Ratio")

        self.results["merchant_analysis"] = {
            "merchant_stats": stats,
            "risky_merchants": risky,
        }

    def _blocked_countries_analysis(self):
        if self.card_df is None:
            return

        # ── Build the blocked list: always start with the built-in list ──
        blocked_list = set(BUILTIN_BLOCKED_COUNTRIES)

        # Merge in any additional countries from an uploaded CSV
        if self.blocked_df is not None:
            blocked_col = None
            for c in ["LIST OF BLOCKED COUNTRIES", "Country", "COUNTRY", "Countries", "COUNTRIES"]:
                if c in self.blocked_df.columns:
                    blocked_col = c
                    break
            if blocked_col is None and len(self.blocked_df.columns):
                blocked_col = self.blocked_df.columns[0]
            if blocked_col:
                extra = self.blocked_df[blocked_col].astype(str).str.strip().str.upper().tolist()
                blocked_list.update(extra)
                self._log("info", f"Merged {len(extra)} countries from uploaded CSV with built-in list")

        blocked_txns = pd.DataFrame()
        if "BIN country" in self.card_df.columns:
            self.card_df["BIN country"] = self.card_df["BIN country"].astype(str).str.upper()
            blocked_txns = self.card_df[self.card_df["BIN country"].isin(blocked_list)]
        if "Country" in self.card_df.columns and len(blocked_txns) == 0:
            self.card_df["Country"] = self.card_df["Country"].astype(str).str.upper()
            blocked_txns = self.card_df[self.card_df["Country"].isin(blocked_list)]

        self.results["blocked_countries"] = {
            "blocked_transactions": blocked_txns,
            "blocked_count":        len(blocked_txns),
            "blocked_list_size":    len(blocked_list),
        }
        self._log("success" if len(blocked_txns) == 0 else "warning",
                  f"Sanctions: {len(blocked_txns)} blocked-country transactions "
                  f"(checking against {len(blocked_list)} countries)")

    def _secure_3d_analysis(self):
        src = None
        for d in [self.card_df, self.apm_df, self.combined_df]:
            if d is not None and "Is 3D" in d.columns:
                src = d; break
        if src is None:
            return

        stats = src.groupby(["Is 3D","Status"]).agg(
            {"Amount": ["count","sum","mean"]} if "Amount" in src.columns else {"Txid": "count"}
        ).round(2)

        non3d = src[src["Is 3D"]=="No"]
        high_non3d = pd.DataFrame()
        if len(non3d) and "Amount" in non3d.columns:
            threshold = non3d["Amount"].quantile(0.9)
            high_non3d = non3d[non3d["Amount"]>threshold].sort_values("Amount", ascending=False)

        # Merchant 3D breakdown
        merchant_3d = pd.DataFrame()
        if "Merchant" in src.columns:
            m3 = src.groupby(["Merchant","Is 3D"]).size().unstack(fill_value=0)
            if "No" in m3.columns and "Yes" in m3.columns:
                m3["Total"] = m3.sum(axis=1)
                m3["Non_3D_Percentage"] = (m3["No"]/m3["Total"]*100).round(2)
                merchant_3d = m3[m3["Non_3D_Percentage"]>50].sort_values("Non_3D_Percentage", ascending=False)

        self.results["secure_3d_analysis"] = {
            "overall_stats":    stats,
            "merchant_3d":      merchant_3d,
            "high_value_non_3d": high_non3d.head(20) if len(high_non3d) else pd.DataFrame(),
        }

    # ── Summary stats for session ─────────────────────────────────
    def get_summary(self) -> dict:
        payout_phones = self.results.get("enhanced_phone_analysis", {}).get("phones_only_payouts", pd.DataFrame())
        payout_emails = self.results.get("enhanced_email_analysis", {}).get("emails_only_payouts", pd.DataFrame())
        blocked       = self.results.get("blocked_countries", {}).get("blocked_count", 0)
        dv            = self.results.get("velocity_violations", {}).get("daily_violations", pd.DataFrame())
        hv            = self.results.get("velocity_violations", {}).get("hourly_violations", pd.DataFrame())
        networks      = self.results.get("payout_only_cross_analysis", {}).get("fraud_networks", pd.DataFrame())

        payout_risk = 0
        if payout_phones is not None and len(payout_phones):
            payout_risk += float(payout_phones["Payout_Amount"].sum())
        if payout_emails is not None and len(payout_emails):
            payout_risk += float(payout_emails["Payout_Amount"].sum())

        total_alerts = (
            len(payout_phones) + len(payout_emails) +
            int(blocked) +
            len(dv) + len(hv)
        )
        critical_alerts = len(payout_phones) + len(payout_emails) + int(blocked)

        return {
            "total_alerts":    total_alerts,
            "critical_alerts": critical_alerts,
            "payout_phones":   len(payout_phones),
            "payout_emails":   len(payout_emails),
            "blocked_txns":    int(blocked),
            "velocity_daily":  len(dv),
            "velocity_hourly": len(hv),
            "fraud_networks":  len(networks),
            "payout_risk_usd": round(payout_risk, 2),
        }
