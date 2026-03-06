# 🛡️  Project Sentinel 2.0 — Complete Setup Guide

> AML & Fraud Hub: Local Docker → Azure Migration Path

---

## 📁  Project Structure

```
Marble/
├── docker-compose.yml          # Orchestrates all services
├── .env                        # Environment variables (edit this)
├── postgres/
│   └── init.sql                # Database schema (auto-runs on first start)
└── streamlit_app/
    ├── Dockerfile
    ├── requirements.txt
    ├── app.py                  # Home page
    ├── .streamlit/
    │   └── config.toml         # Dark theme + port config
    ├── core/
    │   ├── analyzer.py         # All 12 forensic analysis methods
    │   ├── database.py         # PostgreSQL operations (SQLAlchemy)
    │   └── reports.py          # Excel report generation
    └── pages/
        ├── 1_Upload.py         # File upload + analysis pipeline
        ├── 2_Dashboard.py      # Charts and KPIs
        ├── 3_Cases.py          # Case management
        ├── 4_Search.py         # Entity search
        └── 5_History.py        # Session history + re-download
```

---

## ✅  Prerequisites

Install these on your Windows machine (run in PowerShell as Admin):

### 1. Docker Desktop
Download from: https://www.docker.com/products/docker-desktop/
- Install and restart your machine
- Verify: `docker --version` and `docker compose version`

### 2. Git (optional but recommended)
```powershell
winget install Git.Git
```

---

## 🚀  Step-by-Step Setup

### Step 1 — Copy the project files

Place all provided files into your existing project folder:
```
C:\Users\User\OneDrive\Desktop\Marble\
```

Your folder should now look exactly like the Project Structure above.

### Step 2 — Verify the .env file

Open `C:\Users\User\OneDrive\Desktop\Marble\.env` and confirm:
```
POSTGRES_USER=sentinel
POSTGRES_PASSWORD=sentinel_pass
POSTGRES_DB=sentineldb
```
Change the password to something strong before going to production.

### Step 3 — Build and start all containers

Open PowerShell, navigate to the project folder, then run:
```powershell
cd "C:\Users\User\OneDrive\Desktop\Marble"
docker compose up --build -d
```

This will:
1. Pull the official PostgreSQL 15 image
2. Build the Streamlit image (installs all Python dependencies)
3. Start both containers
4. Run `init.sql` to create all database tables automatically
5. Start the Streamlit web server on port 8501

**First build takes ~3-5 minutes** (downloading images + installing packages).

### Step 4 — Open the app

Open your browser and go to:
```
http://localhost:8501
```

You should see the Sentinel home page with the dark theme.

---

## 📋  Day-to-Day Workflow

### Starting the system (after first setup)
```powershell
cd "C:\Users\User\OneDrive\Desktop\Marble"
docker compose up -d
```

### Stopping the system
```powershell
docker compose down
```

### Stopping AND wiping all data (full reset)
```powershell
docker compose down -v
```

### Viewing live logs
```powershell
# All services
docker compose logs -f

# Just the app
docker compose logs -f streamlit

# Just the database
docker compose logs -f postgres
```

### Rebuilding after code changes
```powershell
docker compose up --build -d
```

---

## 📤  How to Run an Analysis

1. **Open** http://localhost:8501 in your browser
2. **Click** "📤 Upload Data" in the sidebar
3. **Upload** your files:
   - `CARD 20.csv` → "Card TM CSV" slot
   - `APM 20.csv`  → "APM TM CSV" slot
   - `BLOCKED COUNTRIES.csv` → "Blocked Countries CSV" slot (optional but recommended)
4. **Click** "🚀 Run Full Forensic Analysis"
5. Wait ~10-60 seconds depending on file size
6. Review the summary, then **download the Excel report**
7. Navigate to **📊 Dashboard** for charts
8. Navigate to **🚨 Cases** to review and action each alert

---

## 🔍  What Gets Analysed

| # | Analysis | Data | What it finds |
|---|----------|------|---------------|
| 1 | BIN Analysis | Card | Top/bottom BINs by approved amount, decline rates |
| 2 | Card Analysis | Card | High-value cards, cards with most declines |
| 3 | Phone Analysis | APM | Payout-only phones (🔴 CRITICAL), high velocity |
| 4 | Email Analysis | Both | Payout-only emails, emails with 3+ cards |
| 5 | Payout-Only Cross | APM | Fraud networks linking payout phones + emails |
| 6 | Recurring Patterns | Card | Cards with rapid-fire transactions |
| 7 | Velocity Violations | Card | Cards exceeding 10/day or 5/hour |
| 8 | Suspicious Timing | Both | Entities always transacting at same time |
| 9 | Merchant Trends | Both | Merchants with unusual volume swings |
| 10| Merchant Analysis | Best | Risky merchants (<30% or >95% approval) |
| 11| Blocked Countries | Card | Transactions from sanctioned nations |
| 12| 3DS Anomaly | Best | Approved >$500 without 3DS authentication |

---

## 📥  Download Reports

Reports can be downloaded in two ways:
1. **Right after analysis** — click the download button on the Upload page
2. **Any time from History** — go to 📜 History → select session → "Generate Excel Report"

The Excel report has **9 sheets**:
- 📊 Executive Summary (KPIs + key findings)
- 🚨 Fraud Cases (all generated alerts)
- 🔴 Payout-Only Phones
- 🔴 Payout-Only Emails
- ⚡ Velocity Violations
- 🌍 Sanctions Hits
- 🔐 3DS Anomalies
- 💳 BIN Analysis
- 🏪 Risky Merchants

---

## 🔐  Case Management

Go to **🚨 Cases** to action each alert:

| Status | Meaning |
|--------|---------|
| `open` | New, unreviewed alert |
| `confirmed_fraud` | Verified fraudulent activity |
| `false_positive` | Legitimate transaction, incorrectly flagged |
| `under_investigation` | Being reviewed |

To update: select the case → choose new status → add notes → Save.

---

## 🗄️  Database Access (Advanced)

Connect directly to PostgreSQL if needed:
```powershell
docker exec -it sentinel_postgres psql -U sentinel -d sentineldb
```

Useful queries:
```sql
-- Count all transactions
SELECT COUNT(*) FROM transactions;

-- Critical open cases
SELECT * FROM fraud_cases WHERE severity='critical' AND status='open';

-- Payout-only case amounts
SELECT entity_value, amount_usd FROM fraud_cases 
WHERE alert_type='payout_only' ORDER BY amount_usd DESC LIMIT 20;

-- Session summary
SELECT * FROM upload_sessions ORDER BY uploaded_at DESC;
```

---

## ⚙️  Configuration

### Changing FX Rates

Edit `streamlit_app/core/analyzer.py`, find `FX_MAP` at the top:
```python
FX_MAP = {
    "ZMW": 0.037, "GHS": 0.082, "KES": 0.0077, ...
}
```
Update any rate and rebuild: `docker compose up --build -d`

### Changing Velocity Thresholds

In `core/analyzer.py`, find `_velocity_rule_analysis`:
```python
def _velocity_rule_analysis(self, daily_limit=10, hourly_limit=5):
```
Change the defaults as needed.

### Adding a New Sanctioned Country

Your `BLOCKED COUNTRIES.csv` file controls this. Just add the country name to the CSV and re-upload. No code changes needed.

---

## 🚢  Moving to Azure (Phase 2)

When ready to move to Azure:

### 1. Create Azure VM
```bash
# Via Azure CLI
az vm create \
  --resource-group sentinel-rg \
  --name sentinel-vm \
  --image Ubuntu2204 \
  --size Standard_B2ms \
  --admin-username azureuser \
  --generate-ssh-keys
```

### 2. Open ports
```bash
az network nsg rule create \
  --resource-group sentinel-rg \
  --nsg-name sentinel-vmNSG \
  --name AllowStreamlit \
  --priority 1001 \
  --destination-port-range 8501 \
  --access Allow
```

### 3. SSH into VM and install Docker
```bash
ssh azureuser@<VM_IP>
curl -fsSL https://get.docker.com | sh
sudo usermod -aG docker azureuser
sudo apt install docker-compose-plugin -y
```

### 4. Copy project to VM
```bash
# From your Windows machine (PowerShell)
scp -r "C:\Users\User\OneDrive\Desktop\Marble" azureuser@<VM_IP>:~/sentinel
```

### 5. Run on Azure
```bash
ssh azureuser@<VM_IP>
cd ~/sentinel
docker compose up -d --build
```

Access via: `http://<VM_IP>:8501`

### 6. Lock down with NSG (security)
Restrict port 8501 to your office/home IPs only in the Azure portal under:
`VM → Networking → Add inbound port rule`

---

## 🐛  Troubleshooting

### "Database not connected" on home page
```powershell
# Check if containers are running
docker ps

# Check postgres logs
docker compose logs postgres
```

### App not loading at localhost:8501
```powershell
# Check if streamlit is running
docker ps | findstr sentinel
docker compose logs streamlit
```

### After editing Python files, changes not showing
```powershell
# The app volume-mounts your code, so changes are live.
# For requirements.txt changes, rebuild:
docker compose up --build -d
```

### Reset everything and start fresh
```powershell
docker compose down -v
docker compose up --build -d
```

---

## 📊  Architecture

```
Browser (localhost:8501)
        │
        ▼
┌───────────────────┐     ┌──────────────────────┐
│  Streamlit App    │────▶│  PostgreSQL 15        │
│  (Python 3.11)    │     │                       │
│                   │     │  Tables:              │
│  Pages:           │     │  - upload_sessions    │
│  - Upload         │     │  - transactions       │
│  - Dashboard      │     │  - fraud_cases        │
│  - Cases          │     │  - analysis_results   │
│  - Search         │     │                       │
│  - History        │     │  Persists across      │
│                   │     │  container restarts   │
└───────────────────┘     └──────────────────────┘
        │
        ▼
  EnhancedFraudDetectionAnalyzer
  (12 forensic analyses, FX normalisation,
   BIN extraction, sanctions check)
```

---

*Built for the Risk & Fraud Department — Project Sentinel 2.0*
