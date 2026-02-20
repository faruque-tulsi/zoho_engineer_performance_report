"""
============================================================
ENGINEER PERFORMANCE ANALYSIS PIPELINE
============================================================
Flow:
  1. Fetch PDF from Zoho Analytics (using View ID)
  2. Send PDF to Claude API for analysis
  3. Generate formatted PDF report (like Dipu Mondal format)
  4. Send PDF to WhatsApp via WhatsApp Cloud API template

Requirements:
    pip install anthropic requests reportlab python-dotenv

Usage:
    python engineer_analysis_pipeline.py \
        --view-id          123456789 \
        --whatsapp-to      919876543210 \
        --whatsapp-template engineer_report \
        --whatsapp-lang    en_US

  --view-id             Zoho Analytics View/Dashboard ID  (required)
  --whatsapp-to         Recipient phone, E.164 without '+' (required)
  --whatsapp-template   Pre-approved WhatsApp template name (required)
  --whatsapp-lang       Template language code (default: en_US)
============================================================
"""

import os
import re
import json
import time
import base64
import random
import requests
from pathlib import Path
from dotenv import load_dotenv

import anthropic
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table,
    TableStyle, PageBreak
)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_JUSTIFY

# ─────────────────────────────────────────────
# Load .env
# ─────────────────────────────────────────────
load_dotenv()

# ─────────────────────────────────────────────
# CONFIGURATION — secrets loaded from .env
# ─────────────────────────────────────────────
ZOHO_DC            = "in"
ZOHO_CLIENT_ID     = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")

ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY")

PHONE_NUMBER_ID    = os.getenv("WHATSAPP_PHONE_ID")
WA_TOKEN           = os.getenv("WA_TOKEN")

OUTPUT_DIR         = Path("./output_reports")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ─────────────────────────────────────────────
# RUNTIME SETTINGS — edit these directly
# ─────────────────────────────────────────────
ZOHO_ORG_ID        = "60016736787"
WORKSPACE_ID       = "256541000000008002"
VIEW_ID            = "256541000007102192"     # Zoho Analytics View/Dashboard ID

TO_NUMBER          = "919749442741"           # Recipient number, E.164 without '+'
WA_TEMPLATE_NAME   = "zoho_engineer_performance_report"  # Pre-approved template name
WA_LANG            = "en"                     # Template language code

GRAPH_VERSION      = "v19.0"

EXPORT_CONFIG = {
    "responseFormat": "pdf",
    "paperSize":      4,           # A4
    "paperStyle":     "Portrait",
    "showTitle":      0,
    "showDesc":       2,
    "zoomFactor":     100,
    "generateTOC":    False,
    "dashboardLayout": 1,
}


# ╔══════════════════════════════════════════════╗
# ║  STEP 1 — FETCH PDF FROM ZOHO ANALYTICS      ║
# ╚══════════════════════════════════════════════╝

def zoho_accounts_base():
    return f"https://accounts.zoho.{ZOHO_DC}"

def zoho_analytics_base():
    return f"https://analyticsapi.zoho.{ZOHO_DC}"

def zoho_headers(access_token: str) -> dict:
    return {
        "Authorization":   f"Zoho-oauthtoken {access_token}",
        "ZANALYTICS-ORGID": str(ZOHO_ORG_ID),
    }

def get_zoho_access_token() -> str:
    """Exchange refresh token for a Zoho access token, with retry + jitter."""
    url  = f"{zoho_accounts_base()}/oauth/v2/token"
    data = {
        "grant_type":    "refresh_token",
        "client_id":     ZOHO_CLIENT_ID,
        "client_secret": ZOHO_CLIENT_SECRET,
        "refresh_token": ZOHO_REFRESH_TOKEN,
    }
    for attempt in range(5):
        if attempt > 0:
            wait = (2 ** attempt) + random.uniform(0.5, 2.0)
            print(f"  [Zoho] Retry {attempt}/4, waiting {wait:.1f}s...")
            time.sleep(wait)
        try:
            r = requests.post(url, data=data, timeout=60)
            r.raise_for_status()
            token = r.json().get("access_token")
            if not token:
                raise ValueError(f"No access_token in response: {r.text}")
            print("[Zoho] Access token obtained.")
            return token
        except requests.HTTPError as e:
            if e.response.status_code == 400 and attempt < 4:
                continue
            raise
    raise RuntimeError("Failed to get Zoho access token after retries.")


def fetch_zoho_pdf() -> bytes:
    """
    Bulk async export:
      1) Create job  — GET /bulk/workspaces/{ws}/views/{view}/data?CONFIG=...
      2) Poll status — GET /bulk/workspaces/{ws}/exportjobs/{jobId}
      3) Download    — GET /bulk/workspaces/{ws}/exportjobs/{jobId}/data
    Returns raw PDF bytes.
    """
    access_token = get_zoho_access_token()
    base         = zoho_analytics_base()

    # Step 1 — create export job
    create_url = (
        f"{base}/restapi/v2/bulk/workspaces/{WORKSPACE_ID}"
        f"/views/{VIEW_ID}/data"
    )
    cr = requests.get(
        create_url,
        headers=zoho_headers(access_token),
        params={"CONFIG": json.dumps(EXPORT_CONFIG)},
        timeout=60,
    )
    cr.raise_for_status()
    job_id = cr.json()["data"]["jobId"]
    print(f"[Zoho] Bulk export job created: {job_id}")

    # Step 2 — poll until complete
    job_url = (
        f"{base}/restapi/v2/bulk/workspaces/{WORKSPACE_ID}"
        f"/exportjobs/{job_id}"
    )
    for _ in range(120):          # up to 10 minutes
        jr   = requests.get(job_url, headers=zoho_headers(access_token), timeout=60)
        jr.raise_for_status()
        code = int(jr.json().get("data", {}).get("jobCode", 0))
        if code in (1001, 1002):  # in-progress
            time.sleep(5)
            continue
        if code == 1004:          # completed
            print("[Zoho] Export job completed.")
            break
        raise RuntimeError(f"Zoho export failed. jobCode={code}  {jr.text[:500]}")

    # Step 3 — download
    dl_url = (
        f"{base}/restapi/v2/bulk/workspaces/{WORKSPACE_ID}"
        f"/exportjobs/{job_id}/data"
    )
    dr = requests.get(dl_url, headers=zoho_headers(access_token), timeout=180)
    dr.raise_for_status()
    print(f"[Zoho] PDF downloaded — {len(dr.content):,} bytes")
    return dr.content




# ╔══════════════════════════════════════════════╗
# ║  STEP 2 — ANALYSE PDF WITH CLAUDE API        ║
# ╚══════════════════════════════════════════════╝

ANALYSIS_PROMPT = """
You are an expert engineer performance analyst for Tulsi Weigh Solutions (India) — 
a weighbridge and industrial weighing equipment company.

Analyse the attached engineer performance dashboard PDF and extract the following data 
in STRICT JSON format. Do not include any text outside the JSON block.

Required JSON structure:
{
  "engineer_name": "string",
  "week_range": "string (e.g. 08-14 Feb 2026)",
  "working_hours": {
    "score": number,
    "max_score": 20,
    "days": [
      {
        "day": "Monday|Tuesday|Wednesday|Thursday|Friday",
        "hours": "HH:MM",
        "check_in": "HH:MM",
        "check_out": "HH:MM",
        "status": "present|absent|exceptional"
      }
    ],
    "week1_score": number,
    "week2_score": number
  },
  "form_quality": {
    "score": number,
    "max_score": 20,
    "examples": [
      {
        "company": "string",
        "machine_no": "string",
        "work_type": "string",
        "problem": "string",
        "work_done": "string",
        "quality_assessment": "good|average|poor|critical"
      }
    ],
    "week1_score": number,
    "week2_score": number
  },
  "feedback": {
    "score": number,
    "max_score": 30,
    "applicable": true|false,
    "ratings": [
      {
        "company": "string",
        "machine_no": "string",
        "rating": number,
        "out_of": 10,
        "date": "string",
        "comment": "string"
      }
    ],
    "week1_score": number,
    "week2_score": number
  },
  "repeat_calls": {
    "score": number,
    "max_score": 30,
    "count": number,
    "calls": [
      {
        "company": "string",
        "machine_no": "string",
        "first_visit": "string",
        "repeat_date": "string",
        "issue_type": "string"
      }
    ]
  },
  "total_score": number,
  "max_possible": number,
  "percentage": number,
  "week1_total": number,
  "week1_max": number,
  "strengths": ["string"],
  "weaknesses": ["string"],
  "immediate_actions": ["string"]
}

Notes:
- If feedback data is absent/zero, set applicable=false, max_possible=70
- Scores out of 70 when feedback not applicable
- Calculate percentage = total_score / max_possible * 100
- Extract ALL form examples even if poorly written
- week1_score fields: use 0 if not available in the PDF
"""


def analyse_pdf_with_claude(pdf_bytes: bytes) -> dict:
    """Send PDF to Claude claude-sonnet-4-5-20250929 and get structured analysis."""
    print("[Claude] Sending PDF for analysis...")
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    pdf_b64 = base64.standard_b64encode(pdf_bytes).decode("utf-8")

    message = client.messages.create(
        model="claude-sonnet-4-5-20250929",
        max_tokens=4096,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "document",
                        "source": {
                            "type":       "base64",
                            "media_type": "application/pdf",
                            "data":       pdf_b64,
                        },
                    },
                    {
                        "type": "text",
                        "text": ANALYSIS_PROMPT,
                    },
                ],
            }
        ],
    )

    raw_text = message.content[0].text.strip()
    print("[Claude] Analysis received.")

    # Strip markdown fences if present
    raw_text = re.sub(r"^```(?:json)?\s*", "", raw_text)
    raw_text = re.sub(r"\s*```$", "", raw_text)

    data = json.loads(raw_text)
    print(f"[Claude] Parsed JSON for: {data.get('engineer_name', 'Unknown')}")
    return data


# ╔══════════════════════════════════════════════╗
# ║  STEP 3 — GENERATE FORMATTED PDF REPORT      ║
# ╚══════════════════════════════════════════════╝

# ── Color palette ──────────────────────────────
C_BLUE        = colors.HexColor("#0066CC")
C_BLUE_LIGHT  = colors.HexColor("#4A90E2")
C_GREEN       = colors.HexColor("#27AE60")
C_ORANGE      = colors.HexColor("#F39C12")
C_RED         = colors.HexColor("#E74C3C")
C_LGRAY       = colors.HexColor("#F5F5F5")
C_MGRAY       = colors.HexColor("#E0E0E0")
C_DARK        = colors.HexColor("#333333")
C_WHITE       = colors.white


def _style(name, **kwargs):
    """Helper: create a named ParagraphStyle."""
    base = getSampleStyleSheet()["Normal"]
    return ParagraphStyle(name, parent=base, **kwargs)


def _tbl(data, col_widths, styles_list):
    """Helper: build and style a Table."""
    t = Table(data, colWidths=col_widths)
    t.setStyle(TableStyle(styles_list))
    return t


def _banner(text, bg_color, font_size=14, text_color=C_WHITE, width=6.8*inch):
    """Solid-colour banner paragraph."""
    p = Paragraph(
        f'<font size="{font_size}" color="white"><b>{text}</b></font>',
        _style("banner_inner", alignment=TA_CENTER)
    )
    t = _tbl([[p]], [width], [
        ("BACKGROUND",  (0,0), (-1,-1), bg_color),
        ("TOPPADDING",  (0,0), (-1,-1), 10),
        ("BOTTOMPADDING",(0,0),(-1,-1), 10),
        ("ALIGN",       (0,0), (-1,-1), "CENTER"),
    ])
    return t


def _info_box(text, bg_color, border_color=None, width=6.8*inch):
    """Coloured info/warning box."""
    p = Paragraph(text, _style("ibox", leftIndent=0))
    bc = border_color or bg_color
    t = _tbl([[p]], [width], [
        ("BACKGROUND",    (0,0),(-1,-1), bg_color),
        ("LEFTPADDING",   (0,0),(-1,-1), 14),
        ("RIGHTPADDING",  (0,0),(-1,-1), 14),
        ("TOPPADDING",    (0,0),(-1,-1), 12),
        ("BOTTOMPADDING", (0,0),(-1,-1), 12),
        ("BOX",           (0,0),(-1,-1), 1.5, bc),
    ])
    return t


def generate_report_pdf(data: dict, output_path: str) -> str:
    """Render the full 4-page analysis PDF from the Claude JSON output."""
    name       = data.get("engineer_name", "Engineer")
    week       = data.get("week_range", "")
    wh         = data.get("working_hours", {})
    fq         = data.get("form_quality", {})
    fb         = data.get("feedback", {})
    rc         = data.get("repeat_calls", {})
    total      = data.get("total_score", 0)
    max_pts    = data.get("max_possible", 100)
    pct        = data.get("percentage", 0)
    w1_total   = data.get("week1_total", 0)
    w1_max     = data.get("week1_max", 70)
    strengths  = data.get("strengths", [])
    weaknesses = data.get("weaknesses", [])

    # Determine status emoji / colour
    if pct >= 80:
        status_text  = "EXCELLENT"
        banner_color = C_GREEN
    elif pct >= 70:
        status_text  = "ACCEPTABLE - IMPROVEMENT NEEDED"
        banner_color = C_ORANGE
    else:
        status_text  = "BELOW STANDARD - URGENT ACTION REQUIRED"
        banner_color = C_RED

    fb_applicable = fb.get("applicable", True)
    score_label   = f"{total}/{max_pts}"

    doc = SimpleDocTemplate(
        output_path,
        pagesize=letter,
        rightMargin=0.6*inch, leftMargin=0.6*inch,
        topMargin=0.5*inch,   bottomMargin=0.5*inch,
    )
    els = []
    S   = getSampleStyleSheet()

    # ── Page 1: Executive summary ───────────────
    # Title block — name on its own line, subtitle clearly below
    els.append(Paragraph(
        name.upper(),
        _style("title", fontSize=20, textColor=C_BLUE, fontName="Helvetica-Bold",
               leading=24, spaceAfter=4, spaceBefore=0)
    ))
    els.append(Paragraph(
        f"Performance Analysis Report | Week 2 ({week})",
        _style("sub", fontSize=10, textColor=colors.HexColor("#666666"),
               leading=14, spaceAfter=12)
    ))

    # Score banner — single cell with line breaks to avoid overlap
    banner_content = Paragraph(
        f'<font size="13" color="white"><b>EXECUTIVE SUMMARY</b></font><br/>'
        f'<br/>'
        f'<font size="34" color="white"><b>{score_label}</b></font><br/>'
        f'<font size="16" color="white"><b>{pct:.1f}%</b></font><br/>'
        f'<br/>'
        f'<font size="11" color="white">{status_text}</font>',
        _style("banner_txt", alignment=TA_CENTER, leading=20)
    )
    els.append(_tbl(
        [[banner_content]],
        [6.8*inch],
        [
            ("BACKGROUND",    (0,0),(-1,-1), banner_color),
            ("ALIGN",         (0,0),(-1,-1), "CENTER"),
            ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
            ("TOPPADDING",    (0,0),(-1,-1), 14),
            ("BOTTOMPADDING", (0,0),(-1,-1), 14),
        ]
    ))
    els.append(Spacer(1, 0.2*inch))

    # Four metric cards
    def _card_col(title, score, max_s, status_txt, good):
        clr = "#27AE60" if good else "#E74C3C"
        return [
            Paragraph(title, _style(f"ct_{title}", fontSize=10, textColor=colors.HexColor("#888888"), alignment=TA_CENTER)),
            Paragraph(f'<font size="28" color="{clr}"><b>{score}/{max_s}</b></font>',
                      _style(f"cv_{title}", alignment=TA_CENTER)),
            Paragraph(f'<font size="9" color="{clr}">{status_txt}</font>',
                      _style(f"cs_{title}", alignment=TA_CENTER)),
        ]

    wh_good = wh.get("score", 0) >= 15
    fq_good = fq.get("score", 0) >= 8
    fb_good = fb.get("applicable", True) and fb.get("score", 0) >= 20
    rc_good = rc.get("score", 0) == 30

    c1 = _card_col("WORKING HOURS", wh.get("score",0), wh.get("max_score",20), "PERFECT" if wh_good else "NEEDS WORK", wh_good)
    c2 = _card_col("FORM QUALITY",  fq.get("score",0), fq.get("max_score",20), "GOOD" if fq_good else "CRITICAL",    fq_good)
    c3_score = "N/A" if not fb_applicable else fq.get("score",0)
    c3 = _card_col("FEEDBACK",      fb.get("score",0) if fb_applicable else "N/A",
                   30 if fb_applicable else "—", "N/A" if not fb_applicable else ("GOOD" if fb_good else "LOW"), fb_good)
    c4 = _card_col("REPEAT CALLS",  rc.get("score",0), rc.get("max_score",30), "PERFECT" if rc_good else "HAS REPEATS", rc_good)

    cards_data = [[c1[0], c2[0], c3[0], c4[0]],
                  [c1[1], c2[1], c3[1], c4[1]],
                  [c1[2], c2[2], c3[2], c4[2]]]
    els.append(_tbl(cards_data, [1.7*inch]*4, [
        ("BACKGROUND",    (0,0),(-1,-1), C_LGRAY),
        ("BOX",           (0,0),(0,-1),  1, C_MGRAY),
        ("BOX",           (1,0),(1,-1),  1, C_MGRAY),
        ("BOX",           (2,0),(2,-1),  1, C_MGRAY),
        ("BOX",           (3,0),(3,-1),  1, C_MGRAY),
        ("ALIGN",         (0,0),(-1,-1), "CENTER"),
        ("TOPPADDING",    (0,0),(-1,0),  10),
        ("BOTTOMPADDING", (0,2),(-1,2),  10),
    ]))
    els.append(Spacer(1, 0.2*inch))

    # Week comparison table
    els.append(Paragraph("WEEK-OVER-WEEK COMPARISON",
        _style("sec", fontSize=16, textColor=C_DARK, fontName="Helvetica-Bold", spaceAfter=10, spaceBefore=14)))

    w1_pct = (w1_total / w1_max * 100) if w1_max else 0
    delta  = total - w1_total
    delta_s = f"+{delta}" if delta >= 0 else str(delta)
    cmp_data = [
        [Paragraph("<b>Metric</b>", S["Normal"]), Paragraph("<b>Week 1</b>", S["Normal"]),
         Paragraph("<b>Week 2</b>", S["Normal"]), Paragraph("<b>Change</b>", S["Normal"])],
        ["Working Hours",
         f'{wh.get("week1_score",0)}/20',
         f'{wh.get("score",0)}/20',
         f'{wh.get("score",0)-wh.get("week1_score",0):+d} pts'],
        ["Form Quality",
         f'{fq.get("week1_score",0)}/20',
         f'{fq.get("score",0)}/20',
         f'{fq.get("score",0)-fq.get("week1_score",0):+d} pts'],
        ["Customer Feedback",
         f'{fb.get("week1_score",0)}/30',
         "0/30 N/A" if not fb_applicable else f'{fb.get("score",0)}/30',
         "–"],
        ["Repeat Calls",
         f'{rc.get("score",0)}/30',
         f'{rc.get("score",0)}/30',
         "–"],
        [Paragraph("<b>TOTAL</b>", S["Normal"]),
         Paragraph(f"<b>{w1_total}/{w1_max} ({w1_pct:.1f}%)</b>", S["Normal"]),
         Paragraph(f"<b>{total}/{max_pts} ({pct:.1f}%)</b>", S["Normal"]),
         Paragraph(f"<b>{delta_s} pts</b>", S["Normal"])],
    ]
    els.append(_tbl(cmp_data, [1.9*inch,1.6*inch,1.6*inch,1.7*inch], [
        ("BACKGROUND", (0,0),(-1,0), C_BLUE),
        ("TEXTCOLOR",  (0,0),(-1,0), C_WHITE),
        ("FONTNAME",   (0,0),(-1,0), "Helvetica-Bold"),
        ("ALIGN",      (0,0),(-1,-1),"CENTER"),
        ("BACKGROUND", (0,1),(-1,4), C_WHITE),
        ("BACKGROUND", (0,5),(-1,5), C_LGRAY),
        ("GRID",       (0,0),(-1,-1), 0.5, C_MGRAY),
        ("TOPPADDING", (0,0),(-1,-1), 8),
        ("BOTTOMPADDING",(0,0),(-1,-1),8),
    ]))
    els.append(Spacer(1, 0.2*inch))

    # Key Insights
    str_lines  = "".join(f"• {s}<br/>" for s in strengths)
    weak_lines = "".join(f"• {w}<br/>" for w in weaknesses)
    insight_txt = (
        f"<b>Strengths:</b><br/>{str_lines}<br/>"
        f"<b>Areas of Concern:</b><br/>{weak_lines}"
    )
    els.append(Paragraph("KEY INSIGHTS",
        _style("kh", fontSize=16, textColor=C_DARK, fontName="Helvetica-Bold", spaceAfter=10, spaceBefore=14)))
    els.append(_info_box(insight_txt, C_LGRAY, C_BLUE))

    # ── Page 2: Working Hours + Form Quality ────
    els.append(PageBreak())
    els.append(Paragraph("DETAILED PERFORMANCE BREAKDOWN",
        _style("dpb", fontSize=18, textColor=C_DARK, fontName="Helvetica-Bold", spaceAfter=12)))

    # Working hours table
    els.append(Paragraph("1. WORKING HOURS",
        _style("wh1", fontSize=14, textColor=C_DARK, fontName="Helvetica-Bold", spaceAfter=8)))

    days_data = [[
        Paragraph("<b>Day</b>", S["Normal"]),
        Paragraph("<b>Check In</b>", S["Normal"]),
        Paragraph("<b>Check Out</b>", S["Normal"]),
        Paragraph("<b>Total Hours</b>", S["Normal"]),
        Paragraph("<b>Status</b>", S["Normal"]),
    ]]
    for d in wh.get("days", []):
        h = d.get("hours","—")
        status = d.get("status","").lower()
        if status == "absent":
            flag = "ABSENT"
            row_bg = colors.HexColor("#FFE5E5")
        elif status == "exceptional":
            flag = f"EXCEPTIONAL! {h}"
            row_bg = colors.HexColor("#E8F8E8")
        else:
            flag = h
            row_bg = colors.HexColor("#E8F5E9")
        days_data.append([
            d.get("day",""),
            d.get("check_in","—"),
            d.get("check_out","—"),
            h,
            flag,
        ])

    n_days = len(days_data) - 1
    row_styles = [
        ("BACKGROUND", (0,0),(-1,0), C_BLUE_LIGHT),
        ("TEXTCOLOR",  (0,0),(-1,0), C_WHITE),
        ("FONTNAME",   (0,0),(-1,0), "Helvetica-Bold"),
        ("ALIGN",      (0,0),(-1,-1),"CENTER"),
        ("GRID",       (0,0),(-1,-1), 0.5, C_MGRAY),
        ("TOPPADDING", (0,0),(-1,-1), 7),
        ("BOTTOMPADDING",(0,0),(-1,-1),7),
    ]
    for i, d in enumerate(wh.get("days", []), 1):
        st = d.get("status","").lower()
        bg = colors.HexColor("#FFE5E5") if st=="absent" else colors.HexColor("#E8F5E9")
        row_styles.append(("BACKGROUND", (0,i),(-1,i), bg))

    els.append(_tbl(days_data, [1.2*inch,1.3*inch,1.3*inch,1.3*inch,1.7*inch], row_styles))
    els.append(Spacer(1, 0.15*inch))

    # Score: 17/20 achieved, etc.
    wh_analysis = (
        f"<b>Score: {wh.get('score',0)}/20</b>  |  "
        f"Week 1: {wh.get('week1_score',0)}/20  →  Week 2: {wh.get('score',0)}/20  "
        f"({wh.get('score',0)-wh.get('week1_score',0):+d} points)"
    )
    els.append(Paragraph(wh_analysis, _style("whs", fontSize=10, textColor=colors.HexColor("#555555"))))
    els.append(Spacer(1, 0.25*inch))

    # Form Quality
    fq_score = fq.get("score", 0)
    fq_color = C_GREEN if fq_score >= 8 else (C_ORANGE if fq_score >= 5 else C_RED)
    els.append(Paragraph(f"2. FORM QUALITY: {fq_score}/20",
        _style("fqt", fontSize=14, textColor=fq_color, fontName="Helvetica-Bold", spaceAfter=8)))

    if fq_score < 5:
        els.append(_banner("CRITICAL - DOCUMENTATION REQUIRES IMMEDIATE IMPROVEMENT", C_RED))
        els.append(Spacer(1, 0.1*inch))

    els.append(Paragraph("<b>Documentation Examples from This Week:</b>",
        _style("fqex", fontSize=11, fontName="Helvetica-Bold", spaceAfter=8)))

    for i, ex in enumerate(fq.get("examples", []), 1):
        qa   = ex.get("quality_assessment","").lower()
        bg   = colors.HexColor("#FFE5E5") if qa in ("poor","critical") else \
               colors.HexColor("#FFF9E6") if qa == "average" else \
               colors.HexColor("#E8F5E9")
        hdr_bg = colors.HexColor("#FFCCCC") if qa in ("poor","critical") else \
                 colors.HexColor("#FFE5B4") if qa == "average" else \
                 colors.HexColor("#CCEECC")
        ex_data = [
            [Paragraph(f"<b>EXAMPLE {i}: {ex.get('company','—')} ({ex.get('machine_no','—')})</b>",
                       _style(f"exhdr{i}", fontSize=10))],
            [Paragraph(f"<b>Work Type:</b> {ex.get('work_type','—')}  |  "
                       f"<b>Quality:</b> {ex.get('quality_assessment','—').upper()}", S["Normal"])],
            [Paragraph(f"<b>Problem:</b> \"{ex.get('problem','—')}\"", S["Normal"])],
            [Paragraph(f"<b>Work Done:</b> \"{ex.get('work_done','—')}\"", S["Normal"])],
        ]
        ex_table = _tbl(ex_data, [6.8*inch], [
            ("BACKGROUND",    (0,0),(-1,0), hdr_bg),
            ("BACKGROUND",    (0,1),(-1,-1),bg),
            ("BOX",           (0,0),(-1,-1), 1, fq_color),
            ("LEFTPADDING",   (0,0),(-1,-1), 10),
            ("RIGHTPADDING",  (0,0),(-1,-1), 10),
            ("TOPPADDING",    (0,0),(-1,-1),  6),
            ("BOTTOMPADDING", (0,0),(-1,-1),  6),
        ])
        els.append(ex_table)
        els.append(Spacer(1, 0.08*inch))

    # ── Page 3: Feedback + Repeat Calls ─────────
    els.append(PageBreak())
    els.append(Paragraph("3. CUSTOMER FEEDBACK",
        _style("fbt", fontSize=14, textColor=C_DARK, fontName="Helvetica-Bold", spaceAfter=8)))

    if not fb_applicable:
        els.append(_banner("NO FEEDBACK DATA COLLECTED — SCORE NOT APPLICABLE", C_ORANGE))
        els.append(Spacer(1, 0.1*inch))
        fb_box = (
            f"<b>Status:</b> No customer feedback collected.<br/><br/>"
            f"<b>Impact:</b> Score calculated out of 70 (not 100). "
            f"If feedback were collected at average level (21/30), "
            f"total score would be {total+21}/{max_pts+30} = "
            f"{(total+21)/(max_pts+30)*100:.1f}%.<br/><br/>"
            f"<b>Action Required:</b> Training on requesting customer feedback "
            f"after every service call."
        )
        els.append(_info_box(fb_box, C_LGRAY, C_ORANGE))
    else:
        ratings = fb.get("ratings", [])
        if ratings:
            fb_data = [[
                Paragraph("<b>Company</b>", S["Normal"]),
                Paragraph("<b>Machine</b>", S["Normal"]),
                Paragraph("<b>Rating</b>", S["Normal"]),
                Paragraph("<b>Date</b>", S["Normal"]),
                Paragraph("<b>Comment</b>", S["Normal"]),
            ]]
            for r in ratings:
                rt = r.get("rating", 0)
                clr = "#27AE60" if rt >= 8 else ("#F39C12" if rt >= 6 else "#E74C3C")
                fb_data.append([
                    r.get("company","—"),
                    r.get("machine_no","—"),
                    Paragraph(f'<font color="{clr}"><b>{rt}/{r.get("out_of",10)}</b></font>', S["Normal"]),
                    r.get("date","—"),
                    Paragraph(r.get("comment","—"), _style("fcmt", fontSize=8)),
                ])
            els.append(_tbl(fb_data, [1.8*inch,1.0*inch,0.8*inch,1.0*inch,2.2*inch], [
                ("BACKGROUND",  (0,0),(-1,0), C_BLUE),
                ("TEXTCOLOR",   (0,0),(-1,0), C_WHITE),
                ("FONTNAME",    (0,0),(-1,0), "Helvetica-Bold"),
                ("ALIGN",       (0,0),(-1,-1),"CENTER"),
                ("GRID",        (0,0),(-1,-1), 0.5, C_MGRAY),
                ("TOPPADDING",  (0,0),(-1,-1), 7),
                ("BOTTOMPADDING",(0,0),(-1,-1),7),
            ]))
    els.append(Spacer(1, 0.25*inch))

    # Repeat Calls
    rc_score = rc.get("score", 0)
    rc_color = C_GREEN if rc_score == 30 else C_RED
    els.append(Paragraph(f"4. REPEAT CALLS: {rc_score}/30",
        _style("rct", fontSize=14, textColor=rc_color, fontName="Helvetica-Bold", spaceAfter=8)))

    if rc_score == 30:
        els.append(_banner("ZERO REPEAT CALLS — PERFECT FIRST-TIME FIX RATE", C_GREEN))
        els.append(Spacer(1, 0.1*inch))
        els.append(_info_box(
            "All customer issues resolved on first visit. "
            "Demonstrates strong technical competence and effective problem-solving.",
            colors.HexColor("#E8F5E9"), C_GREEN
        ))
    else:
        els.append(_banner(f"{rc.get('count',0)} REPEAT CALL(S) RECORDED THIS WEEK", C_RED))
        els.append(Spacer(1, 0.1*inch))
        rc_calls = rc.get("calls", [])
        if rc_calls:
            rc_data = [[
                Paragraph("<b>Company</b>", S["Normal"]),
                Paragraph("<b>Machine</b>", S["Normal"]),
                Paragraph("<b>First Visit</b>", S["Normal"]),
                Paragraph("<b>Repeat Date</b>", S["Normal"]),
                Paragraph("<b>Issue Type</b>", S["Normal"]),
            ]]
            for c in rc_calls:
                rc_data.append([
                    c.get("company","—"), c.get("machine_no","—"),
                    c.get("first_visit","—"), c.get("repeat_date","—"),
                    c.get("issue_type","—"),
                ])
            els.append(_tbl(rc_data, [1.6*inch,1.0*inch,1.2*inch,1.2*inch,1.8*inch], [
                ("BACKGROUND",  (0,0),(-1,0), C_RED),
                ("TEXTCOLOR",   (0,0),(-1,0), C_WHITE),
                ("FONTNAME",    (0,0),(-1,0), "Helvetica-Bold"),
                ("ALIGN",       (0,0),(-1,-1),"CENTER"),
                ("GRID",        (0,0),(-1,-1), 0.5, C_MGRAY),
                ("TOPPADDING",  (0,0),(-1,-1), 7),
                ("BOTTOMPADDING",(0,0),(-1,-1),7),
            ]))

    doc.build(els)
    print(f"[Report] PDF saved: {output_path}")
    return output_path


# ╔══════════════════════════════════════════════╗
# ║  STEP 4 — UPLOAD & SEND VIA WHATSAPP API     ║
# ╚══════════════════════════════════════════════╝

def graph_base() -> str:
    return f"https://graph.facebook.com/{GRAPH_VERSION}"

def wa_headers() -> dict:
    return {"Authorization": f"Bearer {WA_TOKEN}"}


def upload_media_to_whatsapp(file_path: str) -> str:
    """Upload PDF to WhatsApp Media API and return media_id."""
    print("[WhatsApp] Uploading PDF media...")
    url = f"{graph_base()}/{PHONE_NUMBER_ID}/media"
    with open(file_path, "rb") as f:
        files = {"file": (Path(file_path).name, f, "application/pdf")}
        data  = {"messaging_product": "whatsapp", "type": "application/pdf"}
        r = requests.post(url, headers=wa_headers(), data=data, files=files, timeout=120)
    r.raise_for_status()
    media_id = r.json()["id"]
    print(f"[WhatsApp] Media uploaded. ID: {media_id}")
    return media_id


def send_whatsapp_template(media_id: str, filename: str):
    """
    Send the pre-approved template with the PDF as document header.
    Template: zoho_engineer_performance_report
      - header: document
    """
    print(f"[WhatsApp] Sending template to {TO_NUMBER}...")
    url = f"{graph_base()}/{PHONE_NUMBER_ID}/messages"
    payload = {
        "messaging_product": "whatsapp",
        "to":                TO_NUMBER,
        "type":              "template",
        "template": {
            "name":     WA_TEMPLATE_NAME,
            "language": {"code": WA_LANG},
            "components": [
                {
                    "type": "header",
                    "parameters": [
                        {
                            "type": "document",
                            "document": {
                                "id":       media_id,
                                "filename": filename,
                            },
                        }
                    ],
                }
            ],
        },
    }
    r = requests.post(
        url,
        headers={**wa_headers(), "Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=60,
    )
    r.raise_for_status()
    print("[WhatsApp] Template message sent.")
    return r.json()


# ╔══════════════════════════════════════════════╗
# ║  MAIN PIPELINE                               ║
# ╚══════════════════════════════════════════════╝

def run_pipeline():
    print("=" * 55)
    print("  ENGINEER PERFORMANCE ANALYSIS PIPELINE")
    print("=" * 55)

    # ── Step 1: Fetch PDF from Zoho (bulk async) ──
    pdf_bytes = fetch_zoho_pdf()

    # ── Step 2: Analyse with Claude ───────────────
    analysis_data = analyse_pdf_with_claude(pdf_bytes)

    # ── Step 3: Generate formatted report PDF ─────
    engineer_name = analysis_data.get("engineer_name", "engineer")
    week_range    = analysis_data.get("week_range", "unknown_week")
    safe_name     = re.sub(r"[^a-zA-Z0-9_]", "_", engineer_name)
    safe_week     = re.sub(r"[^a-zA-Z0-9_]", "_", week_range)
    output_path   = str(OUTPUT_DIR / f"{safe_name}_{safe_week}_analysis.pdf")

    generate_report_pdf(analysis_data, output_path)

    # ── Step 4: Upload & send via WhatsApp ────────
    media_id = upload_media_to_whatsapp(output_path)
    send_whatsapp_template(
        media_id = media_id,
        filename = Path(output_path).name,
    )

    print("=" * 55)
    print(f"  DONE — Report: {output_path}")
    print("=" * 55)
    return output_path


# ── Entrypoint ─────────────────────────────────
if __name__ == "__main__":
    run_pipeline()