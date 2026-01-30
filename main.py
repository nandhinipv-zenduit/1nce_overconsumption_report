import os
import asyncio
import aiohttp
import requests
import pandas as pd
from datetime import datetime, timedelta
from aiohttp import ClientTimeout, TCPConnector
from aiolimiter import AsyncLimiter
from asyncio import Semaphore
from requests.auth import HTTPBasicAuth
from tqdm import tqdm
from tqdm.asyncio import tqdm_asyncio
import smtplib
from email.message import EmailMessage
from io import BytesIO


# ==========================================================
# GMAIL CONFIG (GLOBAL)
# ==========================================================
SMTP_SERVER = "smtp.gmail.com"
SMTP_PORT = 587

EMAIL_SENDER = os.getenv("gmail_username")
EMAIL_PASSWORD = os.getenv("gmail_pass")

EMAIL_TO = [
    "nandhinipv@zenduit.com", "abidali@gofleet.com","adrianarosas@zenduit.com"
]

if not EMAIL_SENDER or not EMAIL_PASSWORD:
    raise RuntimeError("❌ Gmail credentials not found in environment variables")


# ==========================================================
# CONFIG
# ==========================================================
ONE_NCE = {
    "url": "https://api.1nce.com",
    "username": os.getenv("username"),
    "password": os.getenv("password")
}

BASE_ZENDUIT = "https://trax-admin-service.zenduit.com"
ZENDU_EMAIL = os.getenv("ZENDU_EMAIL")
ZENDU_PASSWORD = os.getenv("ZENDU_PASSWORD")

ZOHO_BASE_URL = "https://www.zohoapis.com/crm/v2"
ZOHO_MODULE = "Subscriptions"

PAGE_SIZE = 100
MAX_RPS = 15
CONCURRENT_REQUESTS = 15
LIST_CONCURRENCY = 6

OUTPUT_EXCEL = r"C:\Users\suppo\PyCharmMiscProject\.venv\Billing_audit_engine\OP\final_overconsumption_report.xlsx"

zenduit_session = requests.Session()

# ==========================================================
# AUTH
# ==========================================================

zoho_crm = {
    "client_id": os.environ.get("ZOHO_CLIENT_ID"),
    "client_secret": os.environ.get("ZOHO_CLIENT_SECRET"),
    "refresh_token": os.environ.get("ZOHO_REFRESH_TOKEN"),
    "api_domain": "https://www.zohoapis.com/crm/v3"
}
def get_1nce_token():
    r = requests.post(
        f"{ONE_NCE['url']}/management-api/oauth/token",
        data={"grant_type": "client_credentials"},
        auth=HTTPBasicAuth(ONE_NCE["username"], ONE_NCE["password"]),
        timeout=30
    )
    r.raise_for_status()
    return r.json()["access_token"]

def get_access_token():
    r = requests.post(
        "https://accounts.zoho.com/oauth/v2/token",
        data={
            "refresh_token": zoho_crm["refresh_token"],
            "client_id": zoho_crm["client_id"],
            "client_secret": zoho_crm["client_secret"],
            "grant_type": "refresh_token"
        },
        timeout=30
    )
    data = r.json()

    if r.status_code != 200 or "access_token" not in data:
        raise RuntimeError(
            f"Zoho OAuth failed | status={r.status_code} | response={data}"
        )

    return data["access_token"]


def zenduit_auth():
    r = zenduit_session.post(
        f"{BASE_ZENDUIT}/Auth/Authenticate",
        json={"Username": ZENDU_EMAIL, "Password": ZENDU_PASSWORD},
        timeout=30
    )
    r.raise_for_status()

    zenduit_session.headers.update({
        "Authorization": f"Bearer {r.json()['Token']}",
        "Content-Type": "application/json"
    })

# ==========================================================
# 1NCE SIM INVENTORY (BASE)
# ==========================================================
def fetch_all_sims(token):
    headers = {"Authorization": f"Bearer {token}"}
    page = 1
    rows = []

    while True:
        r = requests.get(
            f"{ONE_NCE['url']}/management-api/v1/sims",
            headers=headers,
            params={"page": page, "pageSize": PAGE_SIZE},
            timeout=30
        )
        r.raise_for_status()
        batch = r.json()

        if not batch:
            break

        rows.extend(batch)
        if len(batch) < PAGE_SIZE:
            break

        page += 1

    df = pd.DataFrame(rows)
    df["ICCID"] = df["iccid"].astype(str).str.strip()
    return df[["ICCID"]]

# ==========================================================
# 1NCE USAGE (T31–T1)
# ==========================================================
async def fetch_usage(session, limiter, sem, token, iccid, start_dt, end_dt):
    url = f"{ONE_NCE['url']}/management-api/v1/sims/{iccid}/usage"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "start_dt": start_dt.strftime("%Y-%m-%dT00:00:00Z"),
        "end_dt": end_dt.strftime("%Y-%m-%dT23:59:59Z")
    }

    total = 0.0

    async with limiter, sem:
        async with session.get(url, headers=headers, params=params) as r:
            if r.status != 200:
                return {"ICCID": iccid, "1NCE_MB_T31_T1": 0}

            payload = await r.json()
            for s in payload.get("stats", []):
                if s.get("date") == "TOTAL":
                    continue
                total += float(s.get("data", {}).get("volume", 0))

    return {"ICCID": iccid, "1NCE_MB_T31_T1": round(total, 2)}

async def fetch_all_usage(token, iccids, start_dt, end_dt):
    timeout = ClientTimeout(sock_connect=20, sock_read=60)
    connector = TCPConnector(limit=CONCURRENT_REQUESTS)
    limiter = AsyncLimiter(MAX_RPS, 1)
    sem = Semaphore(CONCURRENT_REQUESTS)

    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [
            fetch_usage(session, limiter, sem, token, iccid, start_dt, end_dt)
            for iccid in iccids
        ]

        # ✅ Async progress bar for 1NCE usage
        results = []
        for f in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Fetching 1NCE usage"):
            results.append(await f)

        return pd.DataFrame(results)

# ==========================================================
# ZENDUIT
# ==========================================================
def fetch_zenduit_devices():
    r = zenduit_session.post(f"{BASE_ZENDUIT}/Device/GetAll", json={}, timeout=60)
    r.raise_for_status()

    df = pd.json_normalize(r.json())
    df = df.rename(columns={
        "ICCID": "ICCID",
        "Serial": "Device_Serial",
        "DataPlan": "Zenduit_Data_Plan"
    })

    df["ICCID"] = df["ICCID"].astype(str).str.strip()
    df["Device_Serial"] = df["Device_Serial"].astype(str).str.strip()
    df["Zenduit_Data_Plan"] = pd.to_numeric(df["Zenduit_Data_Plan"], errors="coerce").fillna(0)

    # ✅ Deduplicate here with progress
    df = (
        df.sort_values("Zenduit_Data_Plan", ascending=False)
          .drop_duplicates(subset=["ICCID"], keep="first")
    )

    return df[["ICCID", "Device_Serial", "Zenduit_Data_Plan"]]

def fetch_zenduit_usage(start_dt, end_dt, retries=3, timeout=180):
    payload = {
        "DateFrom": f"{start_dt}T00:00:00Z",
        "DateTo": f"{end_dt}T23:59:59Z"
    }

    for attempt in range(1, retries + 1):
        try:
            print(f"Fetching Zenduit usage (attempt {attempt})...")
            r = zenduit_session.post(
                f"{BASE_ZENDUIT}/DataUsage/Get",
                json=payload,
                timeout=timeout
            )
            r.raise_for_status()

            df = pd.DataFrame([
                {
                    "ICCID": str(d.get("ICCID")).strip(),
                    "Zenduit_Usage_MB": d.get("Usage", 0),
                    "Zenduit_BillingStatus": d.get("BillingStatus")
                }
                for d in r.json()
            ])
            print(f"✅ Zenduit usage fetched: {len(df)} records")
            return df

        except requests.exceptions.ReadTimeout:
            print(f"⏳ Zenduit usage timeout (attempt {attempt}/{retries})")
            if attempt == retries:
                print("⚠️ Zenduit usage failed after retries — returning empty DataFrame")
                return pd.DataFrame(columns=["ICCID", "Zenduit_Usage_MB", "Zenduit_BillingStatus"])

        except Exception as e:
            print(f"❌ Zenduit usage error: {e}")
            return pd.DataFrame(columns=["ICCID", "Zenduit_Usage_MB", "Zenduit_BillingStatus"])

# ==========================================================
# ZOHO
# ==========================================================
def lookup_name(v): return v.get("name") if isinstance(v, dict) else None
def lookup_id(v): return v.get("id") if isinstance(v, dict) else None

async def fetch_zoho_subs(token):
    page = 1
    rows = []

    async with aiohttp.ClientSession() as session:
        while True:
            tasks = [
                session.get(
                    f"{ZOHO_BASE_URL}/{ZOHO_MODULE}",
                    headers={"Authorization": f"Zoho-oauthtoken {token}"},
                    params={"page": p, "per_page": PAGE_SIZE}
                )
                for p in range(page, page + LIST_CONCURRENCY)
            ]

            responses = await asyncio.gather(*tasks)
            stop = False

            for r in tqdm(responses, desc=f"Fetching Zoho subs (page {page})"):
                if r.status != 200:
                    stop = True
                    break

                data = (await r.json()).get("data", [])
                if not data:
                    stop = True
                    break

                for s in data:
                    if (s.get("Status") or "").lower() != "active":
                        continue

                    rows.append({
                        "Device_Serial": str(s.get("Device_Serial")).strip(),
                        "account_id": lookup_id(s.get("Customer_Account")),
                        "account_name": lookup_name(s.get("Customer_Account"))
                    })

                if len(data) < PAGE_SIZE:
                    stop = True


            if stop:
                break
            page += LIST_CONCURRENCY

    return pd.DataFrame(rows)

def send_email(overconsumption_count, unmapped_count, excel_buffer):
    msg = EmailMessage()
    msg["Subject"] = "Monthly SIM Usage Audit – Overconsumption Report"
    msg["From"] = EMAIL_SENDER
    msg["To"] = ", ".join(EMAIL_TO)

    msg.set_content(f"""
Hello Team,

Please find the monthly SIM usage audit report attached.

Summary:
- Overconsumption SIMs: {overconsumption_count}
- ICCIDs without active customer in ZenduOne: {unmapped_count}

Regards,
Nandhiv
""")

    msg.add_attachment(
        excel_buffer.read(),
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="final_overconsumption_report.xlsx"
    )

    with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
        server.starttls()
        server.login(EMAIL_SENDER, EMAIL_PASSWORD)
        server.send_message(msg)

    print("📧 Email sent (Excel attached from memory)")
# ==========================================================
# MAIN
# ==========================================================
async def main():
    print("🔹 Getting 1NCE token...")
    nce_token = get_1nce_token()

    print("🔹 Fetching all SIMs...")
    df_base = fetch_all_sims(nce_token)
    print(f"✅ Total SIMs fetched: {len(df_base)}")

    today = datetime.utcnow().date()
    start_dt = today - timedelta(days=31)
    end_dt = today - timedelta(days=1)

    print("🔹 Fetching 1NCE usage for all SIMs...")
    df_usage = await fetch_all_usage(nce_token, df_base["ICCID"].tolist(), start_dt, end_dt)
    df_base = df_base.merge(df_usage, on="ICCID", how="left")

    print("🔹 Authenticating Zenduit...")
    zenduit_auth()

    print("🔹 Fetching Zenduit devices...")
    df_z_devices = fetch_zenduit_devices()
    print(f"✅ Zenduit devices fetched: {len(df_z_devices)}")

    print("🔹 Fetching Zenduit usage...")
    df_z_usage = fetch_zenduit_usage(start_dt, end_dt)
    df_z_usage = (
        df_z_usage
        .groupby("ICCID", as_index=False)
        .agg(
            Zenduit_Usage_MB=("Zenduit_Usage_MB", "sum"),
            Zenduit_BillingStatus=("Zenduit_BillingStatus", "first")
        )
    )
    df_base = (
        df_base
        .merge(df_z_devices, on="ICCID", how="left")
        .merge(df_z_usage, on="ICCID", how="left")
    )

    print("🔹 Fetching Zoho subscriptions...")
    zoho_token = get_access_token()
    df_zoho = await fetch_zoho_subs(zoho_token)
    df_base = df_base.merge(df_zoho, on="Device_Serial", how="left")
    print(f"✅ Zoho subscriptions fetched: {len(df_zoho)}")

    df_base["Consumption"] = ""
    df_base.loc[
        df_base["1NCE_MB_T31_T1"] > df_base["Zenduit_Data_Plan"],
        "Consumption"
    ] = "Overconsumption"

    # ======================================================
    # 🔹 ADDED LOGIC (NO EXISTING CODE CHANGED)
    # ======================================================
    df_no_customer_in_zenduone = df_base[
        df_base["ICCID"].notna() & (
            df_base["Device_Serial"].isna() |
            (df_base["Device_Serial"].astype(str).str.strip() == "")
        )
    ]

    df_base["account_key"] = df_base["account_id"].fillna(
        "UNMAPPED_" + df_base["ICCID"]
    )

    df_account_summary = (
        df_base[df_base["account_id"].notna()]  # ✅ KEEP ONLY MAPPED ACCOUNTS
        .groupby(["account_id", "account_name"], dropna=False)
        .agg(
            iccid_count=("ICCID", "nunique"),
            one_nce_usage_mb=("1NCE_MB_T31_T1", "sum"),
            zenduit_usage_mb=("Zenduit_Usage_MB", "sum")
        )
        .reset_index()
    )

    print("🔹 Writing Excel file...")
    excel_buffer = BytesIO()

    with pd.ExcelWriter(excel_buffer, engine="xlsxwriter") as writer:
        df_base.to_excel(writer, sheet_name="base_combined", index=False)
        df_no_customer_in_zenduone.to_excel(writer, sheet_name="no_customer_in_zenduone", index=False)
        df_account_summary.to_excel(writer, sheet_name="account_usage_summary", index=False)

    excel_buffer.seek(0)

    print("✅ DONE → Excel generated")


    # ======================================================
    # 🔹 SUMMARY COUNTS
    # ======================================================
    overconsumption_count = df_base[
        df_base["Consumption"] == "Overconsumption"
        ].shape[0]

    unmapped_iccid_count = df_no_customer_in_zenduone.shape[0]

    print(f"📊 Overconsumption count: {overconsumption_count}")
    print(f"📊 ICCIDs without active customer: {unmapped_iccid_count}")

    print("🔹 Sending email...")
    send_email(
        overconsumption_count=overconsumption_count,
        unmapped_count=unmapped_iccid_count,
        excel_buffer=excel_buffer
    )


# ==========================================================
if __name__ == "__main__":
    asyncio.run(main())
