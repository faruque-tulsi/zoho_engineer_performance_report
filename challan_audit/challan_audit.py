"""
Challan Audit Script — Tulsi Weigh Solutions Pvt. Ltd.
=======================================================
Triggered every Saturday via GitHub Actions (or run locally).

Logic:
  1. Fetch all Service tickets via plain /tickets endpoint (no search scope needed).
  2. Filter by createdTime in Python (current calendar month).
  3. For each ticket that has a "Challan Attachment" in its AttachmentHistory:
       a. Read company name from cf_lw_number (ticket custom field).
       b. Read company name from the engineer's comment (Google Form submission).
       c. Ask Claude to compare the two names intelligently.
  4. If MISMATCH → create a Zoho Desk task for Ankita Das Bag.
  5. Save a full JSON audit report as a GitHub Actions artifact.
"""

import os
import re
import json
import logging
import sys
from datetime import datetime, timezone, timedelta
import requests
import base64
import io
try:
    from PIL import Image
except ImportError:
    Image = None  # Handled safely later if missing
import anthropic
from dotenv import load_dotenv  # pip install python-dotenv

# Load .env file when running locally (no-op in GitHub Actions)
load_dotenv()

# ─────────────────────────────────────────────────────────────
# Config — all values come from .env locally / GitHub Secrets in CI
# ─────────────────────────────────────────────────────────────
ZOHO_CLIENT_ID      = os.environ["ZOHO_CLIENT_ID"]
ZOHO_CLIENT_SECRET  = os.environ["ZOHO_CLIENT_SECRET"]
ZOHO_REFRESH_TOKEN  = os.environ["ZOHO_REFRESH_TOKEN"]
ORG_ID              = os.environ["ZOHO_ORG_ID"]           # e.g. "60016737139"
DEPARTMENT_ID       = os.environ["ZOHO_DEPARTMENT_ID"]    # e.g. "86457000002722029"
ANKITA_AGENT_ID     = os.environ["ANKITA_AGENT_ID"]       # Ankita Das Bag's agent ID
ANTHROPIC_API_KEY   = os.environ["ANTHROPIC_API_KEY"]
DATE_RANGE_OVERRIDE = os.environ.get("DATE_RANGE_OVERRIDE", "").strip()

ZOHO_DESK_BASE = "https://desk.zoho.in/api/v1"
ZOHO_TOKEN_URL = "https://accounts.zoho.in/oauth/v2/token"

# ─────────────────────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger("challan_audit")


# ─────────────────────────────────────────────────────────────
# 1.  Zoho OAuth — exchange refresh token for access token
# ─────────────────────────────────────────────────────────────
def get_zoho_access_token() -> str:
    log.info("Obtaining Zoho access token …")
    resp = requests.post(ZOHO_TOKEN_URL, params={
        "grant_type":    "refresh_token",
        "client_id":     ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "refresh_token": ZOHO_REFRESH_TOKEN,
    }, timeout=30)
    resp.raise_for_status()
    token = resp.json().get("access_token")
    if not token:
        raise RuntimeError(f"No access_token in response: {resp.text}")
    log.info("Access token obtained.")
    return token


def zh(access_token: str) -> dict:
    """Return standard Zoho Desk request headers."""
    return {
        "Authorization": f"Zoho-oauthtoken {access_token}",
        "orgId": ORG_ID,
        "Content-Type": "application/json",
    }


# ─────────────────────────────────────────────────────────────
# 2.  Date range — current calendar month (or manual override)
# ─────────────────────────────────────────────────────────────
def get_audit_date_range() -> tuple[str, str]:
    """
    Default  : first day of the current month 00:00 IST → now.
    Override : set DATE_RANGE_OVERRIDE="FROM_ISO,TO_ISO" in .env or GitHub input.
               Example: 2026-03-01T00:00:00.000Z,2026-03-13T23:59:59.000Z
    """
    if DATE_RANGE_OVERRIDE and "," in DATE_RANGE_OVERRIDE:
        parts = DATE_RANGE_OVERRIDE.split(",", 1)
        log.info(f"Using manual date range override: {parts[0]} -> {parts[1]}")
        return parts[0].strip(), parts[1].strip()

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc + timedelta(hours=5, minutes=30)
    
    # Calculate days since Saturday (Monday=0, ... Saturday=5)
    days_since_sat = (now_ist.weekday() - 5) % 7
    last_sat_ist = now_ist - timedelta(days=days_since_sat)
    last_sat_10am_ist = last_sat_ist.replace(hour=10, minute=0, second=0, microsecond=0)
    
    # If the scheduled job triggers right AT Saturday 10:00 AM (e.g., 10:00:05),
    # the time delta will be tiny. We want to audit the *previous* 7 days.
    if (now_ist - last_sat_10am_ist).total_seconds() < 3600:
        last_sat_10am_ist -= timedelta(days=7)

    start_utc = last_sat_10am_ist - timedelta(hours=5, minutes=30)
    
    fmt = "%Y-%m-%dT%H:%M:%S.000Z"
    return start_utc.strftime(fmt), now_utc.strftime(fmt)


# ─────────────────────────────────────────────────────────────
# 3.  Fetch tickets via plain /tickets endpoint
#     Filters by date in Python (avoids 403 from /tickets/search
#     which requires the Desk.search.READ scope)
# ─────────────────────────────────────────────────────────────
def fetch_tickets(access_token: str) -> list[dict]:
    """
    Uses the /tickets/search endpoint to natively filter by modifiedTimeRange.
    This guarantees we catch any old tickets that recently had attachments added.
    """
    from_date, to_date = get_audit_date_range()
    log.info(f"Searching tickets modified between {from_date} -> {to_date}")

    all_tickets = []
    offset = 0

    while True:
        resp = requests.get(
            f"{ZOHO_DESK_BASE}/tickets/search",
            headers=zh(access_token),
            params={
                "departmentId":      DEPARTMENT_ID,
                "limit":             50,
                "from":              offset,
                "modifiedTimeRange": f"{from_date},{to_date}",
            },
            timeout=30,
        )
        if not resp.ok:
            log.error(f"  Zoho API search error {resp.status_code}: {resp.text}")
            resp.raise_for_status()

        batch = resp.json().get("data", [])
        if not batch:
            break

        all_tickets.extend(batch)
        log.info(f"  Offset {offset}: fetched {len(batch)}, total so far: {len(all_tickets)}")

        if len(batch) < 50:
            break
        offset += 50

    log.info(f"Total tickets modified in audit range: {len(all_tickets)}")
    return all_tickets


# -------------------------------------------------------------
# 4.  Check ticket attachments for a challan
# -------------------------------------------------------------
def get_challan_attachment_info(ticket_id: str, access_token: str) -> tuple[str | None, str | None]:
    """
    Lists all attachments on the ticket and returns the (ID, createdTime) of the
    first filename containing 'challan'. This matches 'Challan Attachment.jpg' etc.
    """
    resp = requests.get(
        f"{ZOHO_DESK_BASE}/tickets/{ticket_id}/attachments",
        headers=zh(access_token),
        params={"limit": 50},
        timeout=20,
    )
    if not resp.ok:
        log.warning(f"    Attachments fetch failed for ticket {ticket_id}: HTTP {resp.status_code}")
        return None, None

    attachments = resp.json().get("data", [])
    for att in attachments:
        name = str(att.get("name", "")).lower()
        if "challan" in name:
            return att.get("id"), att.get("createdTime")

    return None, None


# ─────────────────────────────────────────────────────────────
# 5.  Get company names
# ─────────────────────────────────────────────────────────────
def get_ticket_company(ticket_id: str, access_token: str) -> str:
    """
    Fetch full ticket details and return the company name.
    Tries cf_lw_number first (used in Tulsi Weigh tickets),
    then fallback field names.
    """
    resp = requests.get(
        f"{ZOHO_DESK_BASE}/tickets/{ticket_id}",
        headers=zh(access_token),
        params={"orgId": ORG_ID},
        timeout=20,
    )
    if not resp.ok:
        log.warning(f"    Could not fetch ticket {ticket_id}: HTTP {resp.status_code}")
        return ""

    data = resp.json()
    cf   = data.get("cf", {}) or {}

    company = (
        cf.get("cf_lw_number")
        or cf.get("cf_company_name")
        or cf.get("Company Name")
        or ""
    )
    return company.strip()


def get_challan_company_from_comments(ticket_id: str, access_token: str) -> str | None:
    """
    Engineers submit a Google Form → a comment is posted on the ticket containing:
        Company Name : XYZ Industries Ltd
        LW Number    : LW/1234
        ...
    This extracts the Company Name value from that comment text.
    """
    resp = requests.get(
        f"{ZOHO_DESK_BASE}/tickets/{ticket_id}/comments",
        headers=zh(access_token),
        params={"orgId": ORG_ID, "limit": 50},
        timeout=20,
    )
    if not resp.ok:
        return None

    for comment in resp.json().get("data", []):
        html = comment.get("content", "")

        # Strip HTML tags and decode common entities
        text = re.sub(r"<[^>]+>", " ", html)
        text = re.sub(r"&nbsp;",  " ", text)
        text = re.sub(r"&amp;",   "&", text)
        text = re.sub(r"&#39;",   "'", text)
        text = re.sub(r"\s+",     " ", text).strip()

        # Match "Company Name : VALUE" — stop at newline or next field label
        match = re.search(
            r"Company\s*Name\s*[:\-]\s*(.+?)(?=\n|LW\s*Number|Complaint|Tulsi|Engineer|$)",
            text, re.IGNORECASE,
        )
        if match:
            company = match.group(1).strip().rstrip(",;")
            if company:
                return company

    return None


def get_challan_company_from_image(ticket_id: str, attachment_id: str, access_token: str) -> str | None:
    """
    Downloads the actual Challan image and asks Claude Vision to extract the company name.
    """
    log.info("   No comment found -- downloading challan image for OCR...")
    resp = requests.get(
        f"{ZOHO_DESK_BASE}/tickets/{ticket_id}/attachments/{attachment_id}/content",
        headers=zh(access_token),
        params={"orgId": ORG_ID},
        timeout=60,
    )
    if not resp.ok:
        log.warning(f"    Failed to download attachment {attachment_id}")
        return None

    image_bytes = resp.content

    # -- Compress image if > 3MB to avoid Anthropic 5MB base64 limit --
    if Image and len(image_bytes) > 3 * 1024 * 1024:
        try:
            log.info(f"   Image is very large ({len(image_bytes)} bytes) -- compressing...")
            with Image.open(io.BytesIO(image_bytes)) as img:
                if img.mode != "RGB":
                    img = img.convert("RGB")
                img.thumbnail((1600, 1600), Image.Resampling.LANCZOS)
                out_stream = io.BytesIO()
                img.save(out_stream, format="JPEG", quality=85)
                image_bytes = out_stream.getvalue()
                log.info(f"   Compressed down to {len(image_bytes)} bytes")
        except Exception as e:
            log.warning(f"   Could not compress image (passing raw): {e}")

    b64_data = base64.b64encode(image_bytes).decode("utf-8")
    
    try:
        msg = claude_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=100,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {"type": "base64", "media_type": "image/jpeg", "data": b64_data}
                    },
                    {
                        "type": "text",
                        "text": "This is a delivery challan / service report document. Look carefully at the top section, specifically the hand-written field starting with 'M/s' or 'M/S'. This contains the Customer/Consignee/Buyer company name. Extract ONLY this exact full company name. Ignore the 'M/s' prefix itself. Output NOTHING else. If you cannot find a clear company name, output NONE."
                    }
                ]
            }]
        )
        result = msg.content[0].text.strip()
        if result.upper() == "NONE":
            return None
        return result
    except Exception as exc:
        log.warning(f"    Claude Vision OCR failed: {exc}")
        return None


# ─────────────────────────────────────────────────────────────
# 6.  Claude — intelligent company name comparison
# ─────────────────────────────────────────────────────────────
claude_client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

SYSTEM_PROMPT = """You are a strict document auditor for an industrial weighbridge company in India.

Your ONLY job: compare two company names and decide if they refer to the SAME company.

Rules:
- Ignore case differences (ABC = abc = Abc).
- Ignore common legal suffixes: Pvt Ltd, Private Limited, Ltd, P Ltd, Industries,
  Enterprises, Co., Inc., LLP, Works, Corporation, Corp.
- Ignore minor spelling variants, extra spaces, or abbreviations (e.g. "BMW" = "B.M.W.").
- If the core name tokens clearly match → MATCH.
- If the core names are clearly different companies → MISMATCH.

Respond with ONLY valid JSON — no markdown, no explanation, nothing else:
{
  "result": "MATCH" or "MISMATCH",
  "ticket_clean": "<ticket company name normalized>",
  "challan_clean": "<challan company name normalized>",
  "confidence": "HIGH" or "MEDIUM" or "LOW",
  "reason": "<one-line reason>"
}"""


def claude_compare(ticket_company: str, challan_company: str) -> dict:
    """Ask Claude to semantically compare two company names."""
    try:
        msg = claude_client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=300,
            system=SYSTEM_PROMPT,
            messages=[{
                "role": "user",
                "content": (
                    f"Ticket company name : {ticket_company}\n"
                    f"Challan company name: {challan_company}"
                ),
            }],
        )
        raw = msg.content[0].text.strip()
        # Strip accidental markdown fences if model adds them
        raw = re.sub(r"```[a-z]*", "", raw).strip("` \n")
        return json.loads(raw)

    except Exception as exc:
        log.warning(f"    Claude comparison failed ({exc}) — falling back to string compare.")
        match = ticket_company.strip().lower() == challan_company.strip().lower()
        return {
            "result":        "MATCH" if match else "MISMATCH",
            "ticket_clean":  ticket_company,
            "challan_clean": challan_company,
            "confidence":    "LOW",
            "reason":        f"Fallback exact string match (Claude error: {exc})",
        }


# ─────────────────────────────────────────────────────────────
# 7.  Create mismatch task for Ankita Das Bag
# ─────────────────────────────────────────────────────────────
def create_mismatch_task(
    ticket_id: str,
    ticket_number: str,
    ticket_co: str,
    challan_co: str,
    access_token: str,
) -> dict:
    """Create a high-priority task in Zoho Desk assigned to Ankita Das Bag."""
    payload = {
        "departmentId": DEPARTMENT_ID,
        "ticketId":     ticket_id,
        "ownerId":      ANKITA_AGENT_ID,
        "subject":      f"Wrong Challan Attached with Ticket ID #{ticket_number}",
        "description": (
            f"Automated challan audit detected a company name mismatch "
            f"on Ticket #{ticket_number}.\n\n"
            f"• Ticket company name : {ticket_co}\n"
            f"• Challan company name: {challan_co}\n\n"
            "Please review the challan, remove the incorrect one, and attach "
            "the correct challan. Verify with the field engineer if needed."
        ),
        "status":   "Open",
        "priority": "High",
        "dueDate":  (datetime.now(timezone.utc) + timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%S.000Z"),
    }
    resp = requests.post(
        f"{ZOHO_DESK_BASE}/tasks",
        headers=zh(access_token),
        json=payload,
        timeout=20,
    )
    try:
        resp.raise_for_status()
    except requests.exceptions.HTTPError:
        log.error(f"   [FAIL] Task creation failed for #{ticket_number}: {resp.status_code} {resp.text}")
        raise
    task = resp.json()
    log.info(f"    [OK] Task created: ID={task.get('id')} for ticket #{ticket_number}")
    return task


# ─────────────────────────────────────────────────────────────
# 8.  Main audit loop
# ─────────────────────────────────────────────────────────────
def run_audit():
    access_token   = get_zoho_access_token()
    from_date, to_date = get_audit_date_range()
    from_dt        = datetime.fromisoformat(from_date.replace("Z", "+00:00"))
    to_dt          = datetime.fromisoformat(to_date.replace("Z", "+00:00"))
    
    tickets        = fetch_tickets(access_token)

    audit_results  = []
    mismatch_count = 0
    challan_count  = 0
    skip_count     = 0

    for ticket in tickets:
        tid  = ticket["id"]
        tnum = ticket.get("ticketNumber", tid)
        subj = ticket.get("subject", "")[:70]

        log.info(f"-- Ticket #{tnum}: {subj}")

        # -- A: Check attachments for challan -----------
        att_id, att_time_raw = get_challan_attachment_info(tid, access_token)
        if not att_id:
            log.info("   No challan attachment found -- skipping")
            skip_count += 1
            continue

        # -- B: Ensure challan was attached within our target date range --
        if att_time_raw:
            att_dt = datetime.fromisoformat(att_time_raw.replace("Z", "+00:00"))
            if att_dt < from_dt or att_dt > to_dt:
                log.info(f"   Challan attached {att_time_raw.split('T')[0]}, outside audit range -- skipping")
                skip_count += 1
                continue

        challan_count += 1
        log.info("   Challan attachment found -- auditing...")

        # ── C: Company name from ticket field (cf_lw_number) ──
        ticket_co = get_ticket_company(tid, access_token)
        if not ticket_co:
            log.warning(f"   No company name on ticket #{tnum} -- skipping")
            skip_count += 1
            continue

        # ── D: Company name from engineer comment or OCR ──────
        challan_co = get_challan_company_from_comments(tid, access_token)
        if not challan_co:
            challan_co = get_challan_company_from_image(tid, att_id, access_token)
            
        if not challan_co:
            log.warning(f"   No company name found in comments or image for #{tnum} -- skipping")
            skip_count += 1
            continue

        log.info(f"   Ticket company : '{ticket_co}'")
        log.info(f"   Challan company: '{challan_co}'")

        # ── E: Claude comparison ──────────────────────────────
        comparison = claude_compare(ticket_co, challan_co)
        result     = comparison.get("result", "MISMATCH")
        confidence = comparison.get("confidence", "LOW")
        reason     = comparison.get("reason", "")
        log.info(f"   Claude result  : {result} ({confidence}) — {reason}")

        row = {
            "ticket_id":       tid,
            "ticket_number":   tnum,
            "subject":         ticket.get("subject", ""),
            "ticket_company":  ticket_co,
            "challan_company": challan_co,
            "claude_result":   comparison,
            "task_created":    False,
            "task_id":         None,
        }

        # ── F: Create task if mismatch ────────────────────────
        if result == "MISMATCH":
            mismatch_count += 1
            try:
                task = create_mismatch_task(
                    tid, tnum, ticket_co, challan_co, access_token
                )
                row["task_created"] = True
                row["task_id"]      = task.get("id")
            except Exception as exc:
                log.error(f"   [FAIL] Task creation failed for #{tnum}: {exc}")

        audit_results.append(row)

    # ── Summary ───────────────────────────────────────────────
    log.info("=" * 60)
    log.info("AUDIT COMPLETE")
    log.info(f"  Total tickets fetched      : {len(tickets)}")
    log.info(f"  Tickets with challan       : {challan_count}")
    log.info(f"  Skipped (insufficient data): {skip_count}")
    log.info(f"  MISMATCHES found           : {mismatch_count}")
    log.info("=" * 60)

    # ── Save JSON report (uploaded as GitHub Actions artifact) ─
    report = {
        "audit_run_at":  datetime.now(timezone.utc).isoformat(),
        "department_id": DEPARTMENT_ID,
        "org_id":        ORG_ID,
        "summary": {
            "total_tickets":   len(tickets),
            "challan_tickets": challan_count,
            "skipped":         skip_count,
            "mismatches":      mismatch_count,
        },
        "results": audit_results,
    }

    fname = f"audit_report_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
    with open(fname, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)
    log.info(f"Report saved -> {fname}")

    if mismatch_count > 0:
        log.warning(f"{mismatch_count} mismatch(es) detected. Tasks created in Zoho Desk.")


# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    run_audit()