import os
import time
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
import base64
from email.message import EmailMessage
from io import BytesIO
# ==========================================================
# GMAIL CONFIG (GLOBAL) — Gmail API + OAuth2 refresh token
# ==========================================================
GMAIL_CLIENT_ID = os.getenv("GMAIL_CLIENT_ID")
GMAIL_CLIENT_SECRET = os.getenv("GMAIL_CLIENT_SECRET")
GMAIL_REFRESH_TOKEN = os.getenv("GMAIL_REFRESH_TOKEN")
EMAIL_SENDER = os.getenv("GMAIL_USERNAME")  # the mailbox that granted consent
EMAIL_TO = [
    "nandhinipv@zenduit.com","nikithavinod@zenduit.com","abidali@gofleet.com","yaseenshafiq@gofleet.com","rizamae@gofleet.com","amaansardar@zenduit.com"
]
if not all([GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET, GMAIL_REFRESH_TOKEN, EMAIL_SENDER]):
    raise RuntimeError(
        "❌ Gmail OAuth2 credentials not found in environment variables "
        "(need GMAIL_CLIENT_ID, GMAIL_CLIENT_SECRET, GMAIL_REFRESH_TOKEN, GMAIL_USERNAME)"
    )
# ==========================================================
# CONFIG
# ==========================================================
ONE_NCE = {
    "url": "https://api.1nce.com",
    "username": os.getenv("ONE_NCE_USERNAME"),
    "password": os.getenv("ONE_NCE_PASSWORD")
}
BASE_ZENDUIT = "https://trax-admin-service.zenduit.com"
ZENDU_EMAIL = os.getenv("ZENDU_EMAIL")
ZENDU_PASSWORD = os.getenv("ZENDU_PASSWORD")
# FIX: Subscriptions are read with COQL (keyset pagination) instead of the
# page-based Get Records API, which stops at 2,000 records.
ZOHO_COQL_URL = "https://www.zohoapis.com/crm/v3/coql"
ZOHO_MODULE = "Subscriptions"
ZOHO_COQL_LIMIT = 2000
PAGE_SIZE = 100
MAX_RPS = 15
CONCURRENT_REQUESTS = 15
USAGE_RETRIES = 4
OUTPUT_EXCEL = r"C:\Users\suppo\PyCharmMiscProject\.venv\Billing_audit_engine\OP\final_overconsumption_report.xlsx"
zenduit_session = requests.Session()
# ==========================================================
# AUTH
# ==========================================================
zoho_crm = {
    "client_id": os.environ.get("ZOHO_CLIENT_ID"),
    "client_secret": os.environ.get("ZOHO_CLIENT_SECRET"),
    "refresh_token": os.environ.get("ZOHO_REFRESH_TOKEN"),
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
def get_gmail_access_token():
    """Exchange the long-lived refresh token for a short-lived Gmail access token."""
    r = requests.post(
        "https://oauth2.googleapis.com/token",
        data={
            "client_id": GMAIL_CLIENT_ID,
            "client_secret": GMAIL_CLIENT_SECRET,
            "refresh_token": GMAIL_REFRESH_TOKEN,
            "grant_type": "refresh_token"
        },
        timeout=30
    )
    data = r.json()
    if r.status_code != 200 or "access_token" not in data:
        raise RuntimeError(
            f"Gmail OAuth failed | status={r.status_code} | response={data}"
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
    df["iccid"] = df["iccid"].where(df["iccid"].notna(), None)
    df["iccid"] = df["iccid"].astype(str).str.strip()
    df.loc[df["iccid"].isin(["None", "nan", ""]), "iccid"] = None
    return df[["iccid"]].rename(columns={"iccid": "ICCID"})
# ==========================================================
# 1NCE USAGE (T31–T1)
# ==========================================================
RETRYABLE = {429, 500, 502, 503, 504, "error"}
async def fetch_usage(session, limiter, sem, token, iccid, start_dt, end_dt):
    """FIX: a failed call used to be returned as 0 MB, which is
    indistinguishable from a SIM that really used nothing. Now it retries
    rate-limit / server errors and, if it still fails, returns usage=None
    plus the HTTP status so the row can be flagged instead of hidden."""
    url = f"{ONE_NCE['url']}/management-api/v1/sims/{iccid}/usage"
    headers = {"Authorization": f"Bearer {token}"}
    params = {
        "start_dt": start_dt.strftime("%Y-%m-%dT00:00:00Z"),
        "end_dt": end_dt.strftime("%Y-%m-%dT23:59:59Z")
    }
    status, body = None, ""
    for attempt in range(1, USAGE_RETRIES + 1):
        async with limiter, sem:
            try:
                async with session.get(url, headers=headers, params=params) as r:
                    status = r.status
                    if status == 200:
                        payload = await r.json()
                        total = sum(
                            float((s.get("data") or {}).get("volume") or 0)
                            for s in payload.get("stats", [])
                            if s.get("date") != "TOTAL"
                        )
                        return {
                            "ICCID": iccid,
                            "1NCE_MB_T31_T1": round(total, 2),
                            "1NCE_fetch_status": "ok",
                        }
                    body = (await r.text())[:200]
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                status, body = "error", str(e)[:200]
        # back off outside the semaphore so other SIMs keep moving
        if status in RETRYABLE and attempt < USAGE_RETRIES:
            await asyncio.sleep(2 ** attempt)
            continue
        break
    return {
        "ICCID": iccid,
        "1NCE_MB_T31_T1": None,
        "1NCE_fetch_status": f"failed {status}: {body}",
    }
async def fetch_all_usage(token, iccids, start_dt, end_dt):
    iccids = [i for i in iccids if i]  # FIX: don't call /sims/None/usage
    timeout = ClientTimeout(sock_connect=20, sock_read=60)
    connector = TCPConnector(limit=CONCURRENT_REQUESTS)
    limiter = AsyncLimiter(MAX_RPS, 1)
    sem = Semaphore(CONCURRENT_REQUESTS)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        tasks = [
            fetch_usage(session, limiter, sem, token, iccid, start_dt, end_dt)
            for iccid in iccids
        ]
        results = []
        for f in tqdm_asyncio.as_completed(tasks, total=len(tasks), desc="Fetching 1NCE usage"):
            results.append(await f)
        return pd.DataFrame(results, columns=["ICCID", "1NCE_MB_T31_T1", "1NCE_fetch_status"])
# ==========================================================
# ZENDUIT
# ==========================================================
def _zenduit_post_with_retry(path, timeout, retries=4, backoff=15):
    """POST to a Zenduit admin-service endpoint, retrying on transient
    server-side failures (502/503/504 and connection/timeout errors)."""
    last_exc = None
    for attempt in range(1, retries + 1):
        try:
            r = zenduit_session.post(f"{BASE_ZENDUIT}{path}", json={}, timeout=timeout)
            if r.status_code in (502, 503, 504):
                print(
                    f"⏳ Zenduit {path} returned {r.status_code} "
                    f"(attempt {attempt}/{retries}) — retrying in {backoff}s..."
                )
                last_exc = requests.exceptions.HTTPError(
                    f"{r.status_code} from {path}", response=r
                )
                if attempt < retries:
                    time.sleep(backoff)
                continue
            r.raise_for_status()
            return r
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            print(f"⏳ Zenduit {path} connection error (attempt {attempt}/{retries}): {e}")
            last_exc = e
            if attempt < retries:
                time.sleep(backoff)
    raise RuntimeError(f"Zenduit {path} failed after {retries} attempts") from last_exc
def fetch_zenduit_devices():
    print("Fetching Zenduit devices...")
    r = _zenduit_post_with_retry("/Device/GetAll", timeout=120)
    df = pd.json_normalize(r.json())
    df = df.rename(columns={
        "ICCID": "ICCID",
        "Serial": "Device_Serial",
        "DataPlan": "Zenduit_Data_Plan",
        "CompanyId": "CompanyId"
    })
    df["ICCID"] = df["ICCID"].astype(str).str.strip()
    df["Device_Serial"] = df["Device_Serial"].astype(str).str.strip()
    # FIX: astype(str) turns missing values into the strings "None"/"nan"
    df.loc[df["ICCID"].isin(["None", "nan", ""]), "ICCID"] = None
    df.loc[df["Device_Serial"].isin(["None", "nan", ""]), "Device_Serial"] = None
    df["Zenduit_Data_Plan"] = pd.to_numeric(
        df["Zenduit_Data_Plan"],
        errors="coerce"
    ).fillna(0)
    print("Fetching companies...")
    r_comp = _zenduit_post_with_retry("/Company/GetAll", timeout=60)
    df_companies = pd.json_normalize(r_comp.json())
    df_companies = df_companies[["Id", "Name"]].rename(
        columns={
            "Id": "CompanyId",
            "Name": "CompanyName"
        }
    )
    df = df.merge(df_companies, on="CompanyId", how="left")
    df = (
        df[df["ICCID"].notna()]
          .sort_values("Zenduit_Data_Plan", ascending=False)
          .drop_duplicates(subset=["ICCID"], keep="first")
    )
    return df[[
        "ICCID",
        "Device_Serial",
        "Zenduit_Data_Plan",
        "CompanyName"
    ]]
def fetch_zenduit_usage(start_dt, end_dt, retries=3, timeout=180):
    """FIX: used to return an empty DataFrame on failure, which silently set
    every Zenduit usage to 0 and still emailed the report. Now it raises."""
    payload = {
        "DateFrom": f"{start_dt}T00:00:00Z",
        "DateTo": f"{end_dt}T23:59:59Z"
    }
    last_exc = None
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
            ], columns=["ICCID", "Zenduit_Usage_MB", "Zenduit_BillingStatus"])
            print(f"✅ Zenduit usage fetched: {len(df)} records")
            return df
        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectionError) as e:
            print(f"⏳ Zenduit usage timeout/connection error (attempt {attempt}/{retries})")
            last_exc = e
    raise RuntimeError("Zenduit usage failed after retries — report not sent") from last_exc
# ==========================================================
# ZOHO
# ==========================================================
def lookup_name(v): return v.get("name") if isinstance(v, dict) else None
def lookup_id(v): return v.get("id") if isinstance(v, dict) else None
async def fetch_zoho_subs(token):
    """FIX: the old version paged through ALL Subscriptions with page/per_page.
    Zoho only serves the first 2,000 records that way (page_token is required
    beyond that), so ~99% of active subscriptions were never fetched and their
    devices showed up as UNMAPPED.

    This version asks Zoho for Active subscriptions only, via COQL, and pages
    by id ("id > last id seen"), which has no 2,000 / 100,000 ceiling."""
    headers = {"Authorization": f"Zoho-oauthtoken {token}"}
    rows = []
    last_id = 0
    async with aiohttp.ClientSession() as session:
        with tqdm(desc="Fetching Zoho active subscriptions") as bar:
            while True:
                query = (
                    f"select Device_Serial, Customer_Account, Status from {ZOHO_MODULE} "
                    f"where Status = 'Active' and id > {last_id} "
                    f"order by id asc limit {ZOHO_COQL_LIMIT}"
                )
                async with session.post(ZOHO_COQL_URL, headers=headers,
                                        json={"select_query": query}) as r:
                    if r.status == 204:  # no more rows
                        break
                    if r.status != 200:
                        body = await r.text()
                        # FIX: fail loudly — a partial Zoho pull makes every
                        # missing device look UNMAPPED in the report.
                        raise RuntimeError(
                            f"Zoho COQL failed | status={r.status} | body={body[:500]}"
                        )
                    payload = await r.json()
                data = payload.get("data", [])
                for s in data:
                    serial = s.get("Device_Serial")
                    if serial is None:
                        continue
                    rows.append({
                        "Device_Serial": str(serial).strip(),
                        "account_id": lookup_id(s.get("Customer_Account")),
                        "account_name": lookup_name(s.get("Customer_Account")),
                    })
                bar.update(len(data))
                if not data or not payload.get("info", {}).get("more_records"):
                    break
                last_id = int(data[-1]["id"])
    df = pd.DataFrame(rows, columns=["Device_Serial", "account_id", "account_name"])
    # FIX: more than one active subscription per serial would duplicate the
    # ICCID row in the merge and double-count usage in the account summary.
    dupes = df[df.duplicated("Device_Serial", keep=False)]
    if not dupes.empty:
        print(f"⚠️ {dupes['Device_Serial'].nunique()} serials have >1 active subscription — keeping the first")
    return df.drop_duplicates("Device_Serial", keep="first")
def send_email(overconsumption_count, unmapped_count, excel_buffer, nce_failed_count=0):
    msg = EmailMessage()
    msg["Subject"] = "Monthly SIM Usage Audit – Overconsumption Report"
    msg["From"] = EMAIL_SENDER
    msg["To"] = ", ".join(EMAIL_TO)
    msg.set_content(f"""
Hello Team,
Please find the monthly SIM usage audit report attached.
Summary:
- Overconsumption SIMs: {overconsumption_count}
- ICCIDs not found in ZenduOne device list: {unmapped_count}
- SIMs where 1NCE usage could not be fetched: {nce_failed_count}
Regards,
Nandhiv
""")
    msg.add_attachment(
        excel_buffer.read(),
        maintype="application",
        subtype="vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename="final_overconsumption_report.xlsx"
    )
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("ascii")
    access_token = get_gmail_access_token()
    r = requests.post(
        "https://gmail.googleapis.com/gmail/v1/users/me/messages/send",
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        },
        json={"raw": raw},
        timeout=120
    )
    if r.status_code != 200:
        raise RuntimeError(
            f"Gmail send failed | status={r.status_code} | response={r.text}"
        )
    print(f"📧 Email sent via Gmail API (message id: {r.json().get('id')})")
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
    nce_failed = df_base["1NCE_fetch_status"].fillna("").str.startswith("failed")
    print(f"⚠️ 1NCE usage failed for {nce_failed.sum()} SIMs")
    if nce_failed.any():
        print(df_base.loc[nce_failed, "1NCE_fetch_status"].str[:20].value_counts().to_string())
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
    df_base["Zenduit_Usage_MB"] = df_base["Zenduit_Usage_MB"].fillna(0)
    print("🔹 Fetching Zoho subscriptions...")
    zoho_token = get_access_token()
    df_zoho = await fetch_zoho_subs(zoho_token)
    print(f"✅ Zoho active subscriptions fetched: {len(df_zoho)}")
    df_base = df_base.merge(df_zoho, on="Device_Serial", how="left")
    # FIX: only fill usage with 0 where 1NCE actually answered; failed rows
    # are flagged rather than reported as 0 MB.
    nce_ok = ~df_base["1NCE_fetch_status"].fillna("").str.startswith("failed")
    df_base.loc[nce_ok, "1NCE_MB_T31_T1"] = df_base.loc[nce_ok, "1NCE_MB_T31_T1"].fillna(0)
    df_base["Zenduit_Data_Plan"] = df_base["Zenduit_Data_Plan"].fillna(0)
    df_base["Consumption"] = ""
    df_base.loc[
        df_base["1NCE_MB_T31_T1"] > df_base["Zenduit_Data_Plan"],
        "Consumption"
    ] = "Overconsumption"
    df_base.loc[df_base["1NCE_MB_T31_T1"].isna(), "Consumption"] = "1NCE usage fetch failed"
    device_iccids = set(df_z_devices["ICCID"].dropna().astype(str).str.strip())
    df_no_customer_in_zenduone = df_base[
        df_base["ICCID"].notna() &
        ~df_base["ICCID"].astype(str).str.strip().isin(device_iccids)
        ]
    df_base["account_key"] = df_base["account_id"].fillna(
        "UNMAPPED_" + df_base["ICCID"].fillna("")
    )
    df_account_summary = (
        df_base[df_base["account_id"].notna()]
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
    overconsumption_count = (df_base["Consumption"] == "Overconsumption").sum()
    unmapped_iccid_count = df_no_customer_in_zenduone.shape[0]
    nce_failed_count = (df_base["Consumption"] == "1NCE usage fetch failed").sum()
    print(f"📊 Overconsumption count: {overconsumption_count}")
    print(f"📊 ICCIDs not in ZenduOne: {unmapped_iccid_count}")
    print(f"📊 1NCE usage fetch failed: {nce_failed_count}")
    print("🔹 Sending email...")
    send_email(
        overconsumption_count=overconsumption_count,
        unmapped_count=unmapped_iccid_count,
        excel_buffer=excel_buffer,
        nce_failed_count=nce_failed_count,
    )
# ==========================================================
if __name__ == "__main__":
    asyncio.run(main())
