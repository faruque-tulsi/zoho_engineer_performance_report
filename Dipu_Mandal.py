import json
import time
import requests
import os
import random
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ==========================================================
# 1) ZOHO ANALYTICS EXPORT (BULK ASYNC) CONFIG
# ==========================================================
ZOHO_DC = "in"

ZOHO_CLIENT_ID = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")

ZOHO_ORG_ID = "60016736787"
WORKSPACE_ID = "256541000000008002"
VIEW_ID = "256541000007097455"

EXPORT_FILE = Path("Weekly_Performance_Report.pdf")

EXPORT_CONFIG = {
    "responseFormat": "pdf",
    "paperSize": 4,
    "paperStyle": "Portrait",
    "showTitle": 0,
    "showDesc": 2,
    "zoomFactor": 100,
    "generateTOC": False,
    "dashboardLayout": 1
}

# ==========================================================
# 2) META WHATSAPP CLOUD API CONFIG
# ==========================================================
PHONE_NUMBER_ID = "904246956102955"
WA_TOKEN = os.getenv("WA_TOKEN")
TO_NUMBER = "919051956018"
WA_TEMPLATE_NAME = "zoho_engineer_performance_report"
GRAPH_VERSION = "v19.0"
# ==========================================================


# -------------------- ZOHO HELPERS --------------------
def zoho_accounts_base():
    return f"https://accounts.zoho.{ZOHO_DC}"

def zoho_analytics_base():
    return f"https://analyticsapi.zoho.{ZOHO_DC}"

def zoho_get_access_token():
    """Get Zoho access token with retry logic for rate limiting."""
    url = f"{zoho_accounts_base()}/oauth/v2/token"
    data = {
        "grant_type": "refresh_token",
        "client_id": ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "refresh_token": ZOHO_REFRESH_TOKEN,
    }
    
    max_retries = 5
    for attempt in range(max_retries):
        try:
            # Add random jitter to avoid thundering herd
            if attempt > 0:
                jitter = random.uniform(0.5, 2.0)
                wait_time = (2 ** attempt) + jitter
                print(f"Retry {attempt}/{max_retries}, waiting {wait_time:.2f}s...")
                time.sleep(wait_time)
            
            r = requests.post(url, data=data, timeout=60)
            r.raise_for_status()
            return r.json()["access_token"]
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 400 and attempt < max_retries - 1:
                print(f"Rate limited (400), retrying...")
                continue
            raise
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Request failed: {e}, retrying...")
                continue
            raise
    
    raise RuntimeError("Failed to get access token after multiple retries")

def zoho_headers(access_token: str):
    return {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "ZANALYTICS-ORGID": str(ZOHO_ORG_ID),
    }

def zoho_bulk_export_pdf():
    """
    Bulk async export:
      1) Create job: GET /bulk/workspaces/{ws}/views/{view}/data?CONFIG=...
      2) Poll job:   GET /bulk/workspaces/{ws}/exportjobs/{jobId}
      3) Download:   GET /bulk/workspaces/{ws}/exportjobs/{jobId}/data
    """
    access_token = zoho_get_access_token()

    create_url = f"{zoho_analytics_base()}/restapi/v2/bulk/workspaces/{WORKSPACE_ID}/views/{VIEW_ID}/data"
    params = {"CONFIG": json.dumps(EXPORT_CONFIG)}

    cr = requests.get(create_url, headers=zoho_headers(access_token), params=params, timeout=60)
    cr.raise_for_status()
    job_id = cr.json()["data"]["jobId"]
    print("Zoho bulk export jobId:", job_id)

    job_url = f"{zoho_analytics_base()}/restapi/v2/bulk/workspaces/{WORKSPACE_ID}/exportjobs/{job_id}"

    for _ in range(120):  # up to 10 mins
        jr = requests.get(job_url, headers=zoho_headers(access_token), timeout=60)
        jr.raise_for_status()
        data = jr.json().get("data", {})
        job_code = int(data.get("jobCode", 0))

        if job_code in (1001, 1002):  # in progress
            time.sleep(5)
            continue
        if job_code == 1004:          # completed
            break

        raise RuntimeError(f"Zoho bulk export failed. jobCode={job_code}, response={jr.text[:1000]}")

    dl_url = f"{zoho_analytics_base()}/restapi/v2/bulk/workspaces/{WORKSPACE_ID}/exportjobs/{job_id}/data"
    dr = requests.get(dl_url, headers=zoho_headers(access_token), timeout=180)
    dr.raise_for_status()

    EXPORT_FILE.write_bytes(dr.content)
    print(f"✅ Zoho PDF exported: {EXPORT_FILE.resolve()}")


# -------------------- META WHATSAPP HELPERS --------------------
def graph_base():
    return f"https://graph.facebook.com/{GRAPH_VERSION}"

def wa_headers():
    return {"Authorization": f"Bearer {WA_TOKEN}"}

def wa_upload_media(file_path: Path) -> str:
    """
    Upload media:
      POST /{PHONE_NUMBER_ID}/media (multipart/form-data)
    Returns: media id
    """
    url = f"{graph_base()}/{PHONE_NUMBER_ID}/media"

    with file_path.open("rb") as f:
        files = {
            "file": (file_path.name, f, "application/pdf")
        }
        data = {
            "messaging_product": "whatsapp",
            "type": "application/pdf"
        }

        r = requests.post(url, headers=wa_headers(), data=data, files=files, timeout=120)
        r.raise_for_status()
        media_id = r.json()["id"]
        print("✅ WhatsApp media uploaded, media_id:", media_id)
        return media_id

def wa_send_template_with_document(to_number: str, media_id: str, filename: str):
    """
    Send a template message with a document header:
      POST /{PHONE_NUMBER_ID}/messages
    """
    url = f"{graph_base()}/{PHONE_NUMBER_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to": to_number,
        "type": "template",
        "template": {
            "name": WA_TEMPLATE_NAME,
            "language": {
                "code": "en"
            },
            "components": [
                {
                    "type": "header",
                    "parameters": [
                        {
                            "type": "document",
                            "document": {
                                "id": media_id,
                                "filename": filename
                            }
                        }
                    ]
                }
            ]
        }
    }

    r = requests.post(url, headers={**wa_headers(), "Content-Type": "application/json"},
                      data=json.dumps(payload), timeout=60)
    r.raise_for_status()
    print("✅ WhatsApp template message sent.")
    return r.json()


def main():
    # Add initial random delay to avoid all scripts hitting API simultaneously
    initial_delay = random.uniform(0, 5)
    print(f"Starting in {initial_delay:.2f}s to avoid rate limiting...")
    time.sleep(initial_delay)
    
    # 1) Export PDF from Zoho (bulk async)
    zoho_bulk_export_pdf()

    if not EXPORT_FILE.exists() or EXPORT_FILE.stat().st_size == 0:
        raise RuntimeError("Exported PDF file missing or empty.")

    # 2) Upload PDF to WhatsApp
    media_id = wa_upload_media(EXPORT_FILE)

    # 3) Send template message with document header
    wa_send_template_with_document(
        to_number=TO_NUMBER,
        media_id=media_id,
        filename=EXPORT_FILE.name
    )


if __name__ == "__main__":
    main()
