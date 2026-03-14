"""
============================================================
ENGINEER PERFORMANCE ANALYSIS PIPELINE
============================================================
Flow:
  1. Fetch PDF from Zoho Analytics (using View ID)
  2. Send PDF to Claude API for analysis
  3. Generate professional A4 PDF report
  4. Send PDF to WhatsApp via WhatsApp Cloud API template

Requirements:
    pip install anthropic requests reportlab python-dotenv

Usage:
    python engineer_analysis_pipeline.py

Edit the RUNTIME SETTINGS section to change engineer/week config.
============================================================
"""

import os, re, json, time, base64, random
import requests
from pathlib import Path
from dotenv import load_dotenv

import anthropic
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib import colors
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table,
    TableStyle, KeepTogether, HRFlowable
)
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT

load_dotenv()

# ─────────────────────────────────────────────────────────────────────────────
# SECRETS  (all loaded from .env)
# ─────────────────────────────────────────────────────────────────────────────
ZOHO_DC            = "in"
ZOHO_CLIENT_ID     = os.getenv("ZOHO_CLIENT_ID")
ZOHO_CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
ZOHO_REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")

ANTHROPIC_API_KEY  = os.getenv("ANTHROPIC_API_KEY")

PHONE_NUMBER_ID    = os.getenv("WHATSAPP_PHONE_ID")
WA_TOKEN           = os.getenv("WA_TOKEN")

# ─────────────────────────────────────────────────────────────────────────────
# RUNTIME SETTINGS  — edit these for each run
# ─────────────────────────────────────────────────────────────────────────────
ZOHO_ORG_ID        = "60016736787"
WORKSPACE_ID       = "256541000000008002"
VIEW_ID            = "256541000007091754"     # Zoho Analytics View/Dashboard ID

TO_NUMBER          = "916292149257"           # E.164 without '+'
WA_TEMPLATE_NAME   = "zoho_engineer_performance_report"
WA_LANG            = "en"

GRAPH_VERSION      = "v19.0"

OUTPUT_DIR         = Path("./output_reports")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

EXPORT_CONFIG = {
    "responseFormat":  "pdf",
    "paperSize":        4,
    "paperStyle":      "Portrait",
    "showTitle":        0,
    "showDesc":         2,
    "zoomFactor":       100,
    "generateTOC":      False,
    "dashboardLayout":  1,
}

# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE ANALYSIS PROMPT
# ─────────────────────────────────────────────────────────────────────────────
ANALYSIS_PROMPT = """
You are an expert engineer performance analyst for Tulsi Weigh Solutions (India) —
a weighbridge and industrial weighing equipment company.

Analyse the attached engineer performance dashboard PDF and extract the following data
in STRICT JSON format. Do not include any text outside the JSON block.

Scoring rules:
- Working Hours  : /20 based on days > 7 hours
- Form Quality   : /20 based on technical content quality
- Customer Feedback: if any rating < 7 → score = 0/30. If all >=7 → avg/10 * 30.
                     If no feedback data → applicable=false, excluded from total.
- Repeat Calls   : 0 repeats = 30/30, any repeat = 0/30
- If feedback not applicable: max_possible = 70, percentage = total/70 * 100

Required JSON structure:
{
  "engineer_name": "string",
  "week_range": "string (e.g. 08-14 Mar 2026)",
  "report_date": "string",
  "working_hours": {
    "score": number,
    "max_score": 20,
    "days_over_7": number,
    "total_days": number,
    "days": [
      {
        "date": "string",
        "day": "string",
        "check_in": "string",
        "check_out": "string",
        "hours": "string",
        "ok": true or false
      }
    ]
  },
  "form_quality": {
    "score": number,
    "max_score": 20,
    "forms": [
      {
        "account": "string",
        "visit_type": "string",
        "work_done": "string",
        "missing_fields": ["string"]
      }
    ],
    "strengths": ["string"],
    "gaps": ["string"]
  },
  "feedback": {
    "applicable": true or false,
    "score": number,
    "max_score": 30,
    "average_rating": number or null,
    "entries": [
      {
        "account": "string",
        "machine_no": "string",
        "issue_type": "string",
        "date": "string",
        "rating": number,
        "comment": "string"
      }
    ]
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
  "percentage": number
}

Notes:
- Extract ALL form examples even if poorly written
- Mixed Bengali/Hindi/English: evaluate technical content only, not language
- If a field says No Value or is blank, list it in missing_fields
"""


# ╔═══════════════════════════════════════════════════════════════════╗
# ║  STEP 1 — FETCH PDF FROM ZOHO ANALYTICS                          ║
# ╚═══════════════════════════════════════════════════════════════════╝

def _zoho_accounts(): return f"https://accounts.zoho.{ZOHO_DC}"
def _zoho_api():      return f"https://analyticsapi.zoho.{ZOHO_DC}"
def _zoho_hdr(tok):   return {"Authorization": f"Zoho-oauthtoken {tok}",
                               "ZANALYTICS-ORGID": str(ZOHO_ORG_ID)}

def get_zoho_access_token() -> str:
    url  = f"{_zoho_accounts()}/oauth/v2/token"
    data = {"grant_type": "refresh_token", "client_id": ZOHO_CLIENT_ID,
            "client_secret": ZOHO_CLIENT_SECRET, "refresh_token": ZOHO_REFRESH_TOKEN}
    for attempt in range(5):
        if attempt:
            wait = (2 ** attempt) + random.uniform(0.5, 2.0)
            print(f"  [Zoho] Retry {attempt}/4, waiting {wait:.1f}s...")
            time.sleep(wait)
        try:
            r = requests.post(url, data=data, timeout=60)
            r.raise_for_status()
            token = r.json().get("access_token")
            if not token:
                raise ValueError(f"No access_token: {r.text}")
            print("[Zoho] Access token obtained.")
            return token
        except requests.HTTPError as e:
            if e.response.status_code == 400 and attempt < 4:
                continue
            raise
    raise RuntimeError("Failed to get Zoho access token after retries.")


def fetch_zoho_pdf() -> bytes:
    """Bulk async export → poll → download → return PDF bytes."""
    tok  = get_zoho_access_token()
    base = _zoho_api()

    # Create job
    cr = requests.get(
        f"{base}/restapi/v2/bulk/workspaces/{WORKSPACE_ID}/views/{VIEW_ID}/data",
        headers=_zoho_hdr(tok),
        params={"CONFIG": json.dumps(EXPORT_CONFIG)},
        timeout=60,
    )
    cr.raise_for_status()
    job_id = cr.json()["data"]["jobId"]
    print(f"[Zoho] Bulk export job: {job_id}")

    # Poll
    job_url = f"{base}/restapi/v2/bulk/workspaces/{WORKSPACE_ID}/exportjobs/{job_id}"
    for _ in range(120):
        jr   = requests.get(job_url, headers=_zoho_hdr(tok), timeout=60)
        jr.raise_for_status()
        code = int(jr.json().get("data", {}).get("jobCode", 0))
        if code in (1001, 1002):
            time.sleep(5); continue
        if code == 1004:
            print("[Zoho] Export complete."); break
        raise RuntimeError(f"Zoho export failed. jobCode={code}")

    # Download
    dr = requests.get(
        f"{base}/restapi/v2/bulk/workspaces/{WORKSPACE_ID}/exportjobs/{job_id}/data",
        headers=_zoho_hdr(tok), timeout=180,
    )
    dr.raise_for_status()
    print(f"[Zoho] PDF downloaded — {len(dr.content):,} bytes")
    return dr.content


# ╔═══════════════════════════════════════════════════════════════════╗
# ║  STEP 2 — ANALYSE PDF WITH CLAUDE                                ║
# ╚═══════════════════════════════════════════════════════════════════╝

def analyse_pdf_with_claude(pdf_bytes: bytes) -> dict:
    print("[Claude] Sending PDF for analysis...")
    client  = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    pdf_b64 = base64.standard_b64encode(pdf_bytes).decode("utf-8")

    msg = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=4096,
        messages=[{"role": "user", "content": [
            {"type": "document",
             "source": {"type": "base64", "media_type": "application/pdf",
                        "data": pdf_b64}},
            {"type": "text", "text": ANALYSIS_PROMPT},
        ]}],
    )
    raw = msg.content[0].text.strip()
    raw = re.sub(r"^```(?:json)?\s*", "", raw)
    raw = re.sub(r"\s*```$",          "", raw)
    data = json.loads(raw)
    print(f"[Claude] Parsed data for: {data.get('engineer_name', 'Unknown')}")
    return data


# ╔═══════════════════════════════════════════════════════════════════╗
# ║  STEP 3 — GENERATE PROFESSIONAL A4 PDF REPORT                    ║
# ╚═══════════════════════════════════════════════════════════════════╝

# ── Palette ──────────────────────────────────────────────────────────────────
INK       = colors.HexColor("#111111")
DARK      = colors.HexColor("#1C2B3A")
STEEL     = colors.HexColor("#4A6274")
SILVER    = colors.HexColor("#161717")
GHOST     = colors.HexColor("#F0F3F5")
RULE_COL  = colors.HexColor("#D5DDE3")
WHITE     = colors.white

C_BLUE    = colors.HexColor("#1A56A0");  C_BLUE_LT   = colors.HexColor("#EBF2FC")
C_ORANGE  = colors.HexColor("#C85000");  C_ORANGE_LT = colors.HexColor("#FEF3EC")
C_PURPLE  = colors.HexColor("#5C1A8E");  C_PURPLE_LT = colors.HexColor("#F3EDFB")
C_GREEN   = colors.HexColor("#1A6E35");  C_GREEN_LT  = colors.HexColor("#EBF6EF")
C_RED     = colors.HexColor("#B01C1C");  C_RED_LT    = colors.HexColor("#FDF1F1")
C_AMBER   = colors.HexColor("#8B5E00");  C_AMBER_LT  = colors.HexColor("#FFF8E7")


# ── Style / layout helpers ───────────────────────────────────────────────────
def st(name, size=10, bold=False, color=INK, align=TA_LEFT, leading=None):
    fn = "Helvetica-Bold" if bold else "Helvetica"
    return ParagraphStyle(name, fontName=fn, fontSize=size, textColor=color,
                          alignment=align, leading=leading or round(size * 1.4))

def P(text, style):  return Paragraph(str(text), style)
def SP(h_cm):        return Spacer(1, h_cm * cm)
def HR():            return HRFlowable(width="100%", thickness=0.5,
                                        color=RULE_COL, spaceBefore=3, spaceAfter=3)

def _tbl(data, widths, style_cmds):
    t = Table(data, colWidths=widths)
    t.setStyle(TableStyle(style_cmds))
    return t

def sec_hdr(num, title, score_txt, accent, cw):
    t = _tbl([[
        P(str(num),   st("_b", 13, True, WHITE, TA_CENTER)),
        P(title,      st("_h", 13, True, WHITE)),
        P(score_txt,  st("_s", 13, True, WHITE, TA_RIGHT)),
    ]], [0.9*cm, cw - 0.9*cm - 3.6*cm, 3.6*cm], [
        ("BACKGROUND",    (0,0),(-1,-1), accent),
        ("TOPPADDING",    (0,0),(-1,-1), 8),
        ("BOTTOMPADDING", (0,0),(-1,-1), 8),
        ("LEFTPADDING",   (0,0),(-1,-1), 8),
        ("RIGHTPADDING",  (0,0),(-1,-1), 8),
        ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
    ])
    return t

def kv_grid(pairs, accent, lt, cw):
    rows = [[P(k, st("_kk", 9, False, SILVER)),
             P(v, st("_vv", 11, True,  accent))]
            for k, v in pairs]
    t = _tbl(rows, [cw*0.36, cw*0.64], [
        ("BACKGROUND",    (0,0),(-1,-1), lt),
        ("TOPPADDING",    (0,0),(-1,-1), 5),
        ("BOTTOMPADDING", (0,0),(-1,-1), 5),
        ("LEFTPADDING",   (0,0),(-1,-1), 10),
        ("RIGHTPADDING",  (0,0),(-1,-1), 8),
        ("LINEBELOW",     (0,0),(-1,-2), 0.4, RULE_COL),
        ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
    ])
    return t

def finding(icon, text, icon_col, bg, cw):
    t = _tbl([[
        P(icon, st("_fi", 11, True, icon_col, TA_CENTER)),
        P(text, st("_ft", 10, False, INK, leading=14)),
    ]], [0.65*cm, cw - 0.65*cm], [
        ("BACKGROUND",    (0,0),(-1,-1), bg),
        ("TOPPADDING",    (0,0),(-1,-1), 5),
        ("BOTTOMPADDING", (0,0),(-1,-1), 5),
        ("LEFTPADDING",   (0,0),(-1,-1), 8),
        ("RIGHTPADDING",  (0,0),(-1,-1), 8),
        ("VALIGN",        (0,0),(-1,-1), "TOP"),
        ("LINEBELOW",     (0,0),(-1,-1), 0.3, RULE_COL),
        ("LINEBEFORE",    (0,0),(0,-1),  3,   icon_col),
    ])
    return t

def good(text, cw):  return finding("✔", text, C_GREEN,  C_GREEN_LT,  cw)
def bad(text, cw):   return finding("✖", text, C_RED,    C_RED_LT,    cw)
def note(text, cw):  return finding("→", text, C_BLUE,   C_BLUE_LT,   cw)
def warn(text, cw):  return finding("!", text, C_AMBER,  C_AMBER_LT,  cw)

def bar(score, mx, hexc):
    f = round(score / mx * 22)
    return P(f'<font color="#{hexc}">{"█"*f}</font>'
             f'<font color="#D5DDE3">{"█"*(22-f)}</font>',
             st("_bar", 9))


# ── Main report builder ───────────────────────────────────────────────────────
def generate_report_pdf(data: dict, output_path: str) -> str:

    W, H = A4
    ML = 1.4*cm; MR = 1.4*cm
    CW = W - ML - MR

    name     = data.get("engineer_name", "Engineer")
    week     = data.get("week_range",    "")
    rep_date = data.get("report_date",   "")

    wh = data.get("working_hours",  {})
    fq = data.get("form_quality",   {})
    fb = data.get("feedback",       {})
    rc = data.get("repeat_calls",   {})

    WH_S = wh.get("score", 0);  WH_M = wh.get("max_score", 20)
    FQ_S = fq.get("score", 0);  FQ_M = fq.get("max_score", 20)
    FB_S = fb.get("score", 0);  FB_M = fb.get("max_score", 30)
    RC_S = rc.get("score", 0);  RC_M = rc.get("max_score", 30)

    fb_applicable = fb.get("applicable", True)
    TOTAL     = data.get("total_score",  WH_S + FQ_S + (FB_S if fb_applicable else 0) + RC_S)
    MAX_SCORE = data.get("max_possible", 100 if fb_applicable else 70)
    PCT       = data.get("percentage",   round(TOTAL / MAX_SCORE * 100, 1))

    if   PCT >= 85: RATING = "EXCELLENT PERFORMANCE!"; T_ACCENT = C_GREEN
    elif PCT >= 70: RATING = "GOOD PERFORMANCE";        T_ACCENT = C_BLUE
    elif PCT >= 55: RATING = "AVERAGE PERFORMANCE";     T_ACCENT = C_AMBER
    else:           RATING = "NEEDS IMPROVEMENT";       T_ACCENT = C_RED

    story = []

    # ── HEADER ────────────────────────────────────────────────────────────────
    hdr = _tbl([
        [P("ENGINEER PERFORMANCE REPORT",
           st("tg", 9, False, colors.HexColor("#8AAFC8"), TA_CENTER))],
        [P(name, st("nm", 30, True, WHITE, TA_CENTER, leading=36))],
        [P(f"Week:  {week}", st("wk", 12, False, colors.HexColor("#8AAFC8"), TA_CENTER))],
    ], [CW], [
        ("BACKGROUND",    (0,0),(-1,-1), DARK),
        ("TOPPADDING",    (0,0),(-1,-1), 16),
        ("BOTTOMPADDING", (0,0),(-1,-1), 16),
        ("LEFTPADDING",   (0,0),(-1,-1), 0),
        ("RIGHTPADDING",  (0,0),(-1,-1), 0),
    ])
    story.append(hdr)

    # Blue accent strip
    story.append(_tbl([[""]], [CW], [
        ("BACKGROUND",    (0,0),(-1,-1), C_BLUE),
        ("TOPPADDING",    (0,0),(-1,-1), 3),
        ("BOTTOMPADDING", (0,0),(-1,-1), 3),
    ]))
    story.append(SP(0.2))

    # Meta bar
    story.append(_tbl([[
        P("Tulsi Weigh Solutions Pvt. Ltd.", st("mc", 10, False, STEEL)),
        P(f"Week:  {week}",  st("mw", 10, False, STEEL, TA_CENTER)),
        P(f"Report Date:  {rep_date}", st("md", 10, False, STEEL, TA_RIGHT)),
    ]], [CW/3]*3, [
        ("BACKGROUND",    (0,0),(-1,-1), GHOST),
        ("TOPPADDING",    (0,0),(-1,-1), 6),
        ("BOTTOMPADDING", (0,0),(-1,-1), 6),
        ("LEFTPADDING",   (0,0),(-1,-1), 10),
        ("RIGHTPADDING",  (0,0),(-1,-1), 10),
        ("LINEAFTER",     (0,0),(1,-1),  0.5, RULE_COL),
    ]))
    story.append(SP(0.3))

    # ── TOTAL SCORE HERO ──────────────────────────────────────────────────────
    score_label = f"{int(TOTAL)} / {MAX_SCORE}"
    fb_note = "  (Feedback N/A — scored /70)" if not fb_applicable else ""

    inner_left = _tbl([
            [P("TOTAL SCORE", st("tsl", 10, False, colors.HexColor("#8AAFC8"), TA_CENTER))],
            [P(score_label,  st("tsn", 38, True, WHITE, TA_CENTER, leading=44))],
            [P(f"{PCT}%{fb_note}", st("tsp", 11, True, WHITE, TA_CENTER))],
        ], [CW*0.45], [
            ("TOPPADDING",(0,0),(-1,-1),0),("BOTTOMPADDING",(0,0),(-1,-1),0),
            ("LEFTPADDING",(0,0),(-1,-1),0),("RIGHTPADDING",(0,0),(-1,-1),0),
        ])
    inner_right = _tbl([
            [P("PERFORMANCE RATING", st("rl", 9, False, colors.HexColor("#8AAFC8")))],
            [P(RATING,  st("rv", 17, True, WHITE, leading=22))],
            [SP(0.05)],
            [P("Score Breakdown", st("rb", 9, False, colors.HexColor("#8AAFC8")))],
            [P(f"Hours {WH_S}/20  •  Forms {FQ_S}/20  •  "
               f"{'N/A' if not fb_applicable else f'Feedback {int(FB_S)}/30'}  •  "
               f"Calls {RC_S}/30", st("rd", 9.5, False, WHITE))],
        ], [CW*0.55], [
            ("TOPPADDING",(0,0),(-1,-1),0),("BOTTOMPADDING",(0,0),(-1,-1),0),
            ("LEFTPADDING",(0,0),(-1,-1),0),("RIGHTPADDING",(0,0),(-1,-1),0),
        ])
    hero = _tbl([[inner_left, inner_right]], [CW*0.45, CW*0.55], [
        ("BACKGROUND",    (0,0),(-1,-1), T_ACCENT),
        ("TOPPADDING",    (0,0),(-1,-1), 14),
        ("BOTTOMPADDING", (0,0),(-1,-1), 14),
        ("LEFTPADDING",   (0,0),(-1,-1), 14),
        ("RIGHTPADDING",  (0,0),(-1,-1), 14),
        ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
        ("LINEAFTER",     (0,0),(0,-1),  0.7, colors.HexColor("#FFFFFF30")),
    ])
    story.append(hero)
    story.append(SP(0.3))

    # ── SCORE SUMMARY TABLE ───────────────────────────────────────────────────
    story.append(P("Score Summary", st("sh", 11, True, DARK)))
    story.append(SP(0.1))

    fb_score_disp = "N/A" if not fb_applicable else f"{FB_S:.2f}"
    fb_pct_disp   = "N/A" if not fb_applicable else f"{round(FB_S/FB_M*100)}%"

    srows = [
        ("Working Hours",     WH_S,             20,  f"{round(WH_S/20*100)}%",  bar(WH_S, 20, "1A56A0")),
        ("Form Quality",      FQ_S,             20,  f"{round(FQ_S/20*100)}%",  bar(FQ_S, 20, "C85000")),
        ("Customer Feedback", fb_score_disp,    30,  fb_pct_disp,               bar(FB_S, 30, "5C1A8E") if fb_applicable else P("NOT APPLICABLE", st("na", 9, True, C_AMBER))),
        ("Repeat Calls",      RC_S,             30,  "100%",                    bar(RC_S, 30, "1A6E35")),
    ]
    acc_colors = [C_BLUE, C_ORANGE, C_PURPLE, C_GREEN]

    tdata = [[P(h, st(f"sh{i}", 9, True, WHITE, TA_CENTER))
              for i, h in enumerate(["CATEGORY", "SCORE", "MAX", "PCT", "PROGRESS"])]]
    for i, (cat, sc, mx, pct_s, b) in enumerate(srows):
        a = acc_colors[i]
        tdata.append([
            P(cat,    st(f"ca{i}", 11, True, a)),
            P(str(sc),st(f"sc{i}", 13, True, a, TA_CENTER)),
            P(str(mx),st(f"mx{i}", 10, False, SILVER, TA_CENTER)),
            P(pct_s,  st(f"pc{i}", 11, True, a, TA_CENTER)),
            b,
        ])
    tdata.append([
        P("TOTAL",         st("ta", 12, True, WHITE)),
        P(f"{int(TOTAL)}", st("tb", 16, True, WHITE, TA_CENTER)),
        P(f"{MAX_SCORE}",  st("tc", 10, False, colors.HexColor("#8AAFC8"), TA_CENTER)),
        P(f"{PCT}%",       st("td", 13, True, WHITE, TA_CENTER)),
        P(RATING,          st("te", 10, True, WHITE)),
    ])

    sum_tbl = Table(tdata, colWidths=[CW*0.28, CW*0.12, CW*0.09, CW*0.11, CW*0.40],
                    repeatRows=1)
    sum_tbl.setStyle(TableStyle([
        ("BACKGROUND",    (0,0),(-1,0),   DARK),
        ("BACKGROUND",    (0,-1),(-1,-1), T_ACCENT),
        ("ROWBACKGROUNDS",(0,1),(-1,-2),  [WHITE, GHOST]),
        ("GRID",          (0,0),(-1,-2),  0.5, RULE_COL),
        ("LINEABOVE",     (0,-1),(-1,-1), 1.5, DARK),
        ("TOPPADDING",    (0,0),(-1,-1),  7),
        ("BOTTOMPADDING", (0,0),(-1,-1),  7),
        ("LEFTPADDING",   (0,0),(-1,-1),  10),
        ("RIGHTPADDING",  (0,0),(-1,-1),  8),
        ("VALIGN",        (0,0),(-1,-1),  "MIDDLE"),
        ("ALIGN",         (0,0),(-1,0),   "CENTER"),
    ]))
    story.append(sum_tbl)
    story.append(SP(0.45))

    # ═══════════════════════════════════════════════════════════════════
    # SECTION 1 — WORKING HOURS
    # ═══════════════════════════════════════════════════════════════════
    days_over = wh.get("days_over_7", 0)
    total_days = wh.get("total_days", 5)

    story.append(KeepTogether([
        sec_hdr("1", "WORKING HOURS", f"{WH_S} / {WH_M}", C_BLUE, CW),
        SP(0.15),
        kv_grid([
            ("Working Days This Week",  f"{total_days} Days"),
            ("Days Logged > 7 Hours",   f"{days_over} out of {total_days} Days"),
            ("Days Below 7-Hour Target",f"{total_days - days_over} Days"),
            ("Score Awarded",           f"{WH_S} / {WH_M}  —  {round(WH_S/WH_M*100)}%"),
        ], C_BLUE, C_BLUE_LT, CW),
        SP(0.15),
    ]))

    # Daily attendance table
    day_rows = wh.get("days", [])
    if day_rows:
        att_h = ["Date", "Day", "Check-In", "Check-Out", "Hours", "Status"]
        att_data = [[P(h, st(f"ah{i}", 9, True, WHITE, TA_CENTER))
                     for i, h in enumerate(att_h)]]
        row_bgs = []
        for idx, d in enumerate(day_rows):
            ok  = d.get("ok", True)
            row_bgs.append(("BACKGROUND", (0, idx+1), (-1, idx+1),
                            C_GREEN_LT if ok else C_RED_LT))
            att_data.append([
                P(d.get("date",""),     st("ad",  9.5, False, INK)),
                P(d.get("day",""),      st("ady", 9.5, False, INK)),
                P(d.get("check_in","—"),st("aci", 9.5, True,  INK, TA_CENTER)),
                P(d.get("check_out","—"),st("aco",9.5, True,  INK, TA_CENTER)),
                P(d.get("hours","—"),   st("aho", 10,  True,  C_GREEN if ok else C_RED, TA_CENTER)),
                P("✔  > 7 hrs" if ok else "✖  Absent / Below",
                  st("ast", 9.5, True, C_GREEN if ok else C_RED, TA_CENTER)),
            ])
        att_col = [CW*0.19, CW*0.15, CW*0.13, CW*0.13, CW*0.14, CW*0.26]
        att_tbl = Table(att_data, colWidths=att_col, repeatRows=1)
        att_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0,0),(-1,0),  C_BLUE),
            ("GRID",          (0,0),(-1,-1), 0.4, RULE_COL),
            ("TOPPADDING",    (0,0),(-1,-1), 6),
            ("BOTTOMPADDING", (0,0),(-1,-1), 6),
            ("LEFTPADDING",   (0,0),(-1,-1), 7),
            ("RIGHTPADDING",  (0,0),(-1,-1), 7),
            ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
        ] + row_bgs))
        story.append(att_tbl)
        story.append(SP(0.12))

    if days_over == total_days:
        story.append(good(f"All {total_days} working days exceeded the 7-hour threshold — "
                          "perfect attendance and productivity.", CW))
    elif days_over >= total_days * 0.7:
        story.append(good(f"{days_over} of {total_days} days exceeded the 7-hour threshold — "
                          "solid field presence this week.", CW))
    else:
        story.append(bad(f"Only {days_over} of {total_days} days exceeded the 7-hour threshold — "
                         "consistency needs improvement.", CW))

    if total_days >= 6:
        story.append(note("Saturday worked — demonstrates commitment beyond standard 5-day schedule.", CW))
    story.append(SP(0.45))

    # ═══════════════════════════════════════════════════════════════════
    # SECTION 2 — FORM QUALITY
    # ═══════════════════════════════════════════════════════════════════
    story.append(sec_hdr("2", "SERVICE FORM QUALITY", f"{FQ_S} / {FQ_M}", C_ORANGE, CW))
    story.append(SP(0.15))

    forms = fq.get("forms", [])
    if forms:
        fq_h = ["Account", "Visit Type", "Work Done"]
        fq_data = [[P(h, st(f"fh{i}", 9, True, WHITE, TA_CENTER))
                    for i, h in enumerate(fq_h)]]
        for fi, f in enumerate(forms):
            fq_data.append([
                P(f.get("account",""),    st("fa", 10, True,  C_ORANGE)),
                P(f.get("visit_type",""), st("fb", 10, False, INK)),
                P(f.get("work_done",""),  st("fc", 9.5, False, INK, leading=14)),
            ])
        fq_tbl = Table(fq_data,
                       colWidths=[CW*0.22, CW*0.20, CW*0.58],
                       repeatRows=1)
        fq_tbl.setStyle(TableStyle([
            ("BACKGROUND",    (0,0),(-1,0),  C_ORANGE),
            ("ROWBACKGROUNDS",(0,1),(-1,-1), [WHITE, C_ORANGE_LT]),
            ("GRID",          (0,0),(-1,-1), 0.5, RULE_COL),
            ("TOPPADDING",    (0,0),(-1,-1), 8),
            ("BOTTOMPADDING", (0,0),(-1,-1), 8),
            ("LEFTPADDING",   (0,0),(-1,-1), 8),
            ("RIGHTPADDING",  (0,0),(-1,-1), 8),
            ("VALIGN",        (0,0),(-1,-1), "TOP"),
        ]))
        story.append(fq_tbl)
        story.append(SP(0.12))

    for s in fq.get("strengths", []):
        story.append(good(s, CW))
    story.append(note("Mixed-language entries (Bengali / Hindi / English) evaluated on technical "
                      "content only — language quality does not affect scoring.", CW))
    story.append(SP(0.45))

    # ═══════════════════════════════════════════════════════════════════
    # SECTION 3 — CUSTOMER FEEDBACK
    # ═══════════════════════════════════════════════════════════════════
    fb_score_txt = "NOT APPLICABLE" if not fb_applicable else f"{FB_S:.2f} / {FB_M}"
    story.append(KeepTogether([
        sec_hdr("3", "CUSTOMER FEEDBACK", fb_score_txt,
                C_AMBER if not fb_applicable else C_PURPLE, CW),
        SP(0.15),
    ]))

    if not fb_applicable:
        story.append(kv_grid([
            ("Feedback Status",       "NOT APPLICABLE — No feedback data collected"),
            ("Scoring Adjustment",    f"Total scored out of {MAX_SCORE} (not 100)"),
            ("Impact",                "Feedback component excluded from final score"),
            ("Action Required",       "Request customer feedback after every service call"),
        ], C_AMBER, C_AMBER_LT, CW))
        story.append(SP(0.12))
        story.append(warn("No customer feedback was collected this week. "
                          "Actively requesting feedback after each visit is mandatory.", CW))
    else:
        avg = fb.get("average_rating")
        entries = fb.get("entries", [])

        kv_pairs = [
            ("Feedback Entries",      f"{len(entries)} {'Entry' if len(entries)==1 else 'Entries'}"),
            ("Feedback Score",        f"{FB_S:.2f} / {FB_M}  —  {round(FB_S/FB_M*100)}%"),
        ]
        if avg:
            kv_pairs.insert(1, ("Average Customer Rating",
                                f"{avg:.1f} / 10  (Threshold: ≥ 7)"))
        if avg:
            kv_pairs.append(("Calculation",
                             f"({avg:.1f} ÷ 10) × 30  =  {FB_S:.2f} / 30"))
        story.append(kv_grid(kv_pairs, C_PURPLE, C_PURPLE_LT, CW))
        story.append(SP(0.12))

        if entries:
            cf_h = ["Account Name", "Machine No.", "Issue Type", "Date", "Rating", "Valid"]
            cf_data = [[P(h, st(f"cfh{i}", 9, True, WHITE, TA_CENTER))
                        for i, h in enumerate(cf_h)]]
            all_valid = True
            for e in entries:
                r = e.get("rating", 0)
                valid = r >= 7
                if not valid: all_valid = False
                cf_data.append([
                    P(e.get("account",""),    st("cfa", 10, True,  C_PURPLE)),
                    P(e.get("machine_no",""), st("cfb", 10, False, INK,    TA_CENTER)),
                    P(e.get("issue_type",""), st("cfc", 10, False, INK)),
                    P(e.get("date",""),       st("cfd", 10, False, SILVER, TA_CENTER)),
                    P(f"{r} / 10",            st("cfe", 11, True,  C_GREEN if valid else C_RED, TA_CENTER)),
                    P("YES" if valid else "NO",st("cff",11, True,  C_GREEN if valid else C_RED, TA_CENTER)),
                ])
            cf_tbl = Table(cf_data,
                           colWidths=[CW*0.26, CW*0.12, CW*0.22, CW*0.14, CW*0.13, CW*0.13],
                           repeatRows=1)
            cf_tbl.setStyle(TableStyle([
                ("BACKGROUND",    (0,0),(-1,0),  C_PURPLE),
                ("ROWBACKGROUNDS",(0,1),(-1,-1), [WHITE, C_PURPLE_LT]),
                ("GRID",          (0,0),(-1,-1), 0.5, RULE_COL),
                ("TOPPADDING",    (0,0),(-1,-1), 8),
                ("BOTTOMPADDING", (0,0),(-1,-1), 8),
                ("LEFTPADDING",   (0,0),(-1,-1), 8),
                ("RIGHTPADDING",  (0,0),(-1,-1), 8),
                ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
            ]))
            story.append(cf_tbl)
            story.append(SP(0.12))

            for e in entries:
                r = e.get("rating", 0)
                fn = good if r >= 7 else bad
                story.append(fn(
                    f"{e.get('account','')} ({e.get('machine_no','')}): "
                    f"Rating {r}/10 — {e.get('comment','')}", CW))

            if FB_S == 0 and fb_applicable:
                story.append(bad("One or more ratings fell below the threshold of 7 — "
                                 "feedback score is 0/30 per policy.", CW))
            else:
                story.append(good("All feedback ratings meet or exceed the minimum threshold "
                                  "of 7 — feedback is valid and included in scoring.", CW))

    story.append(SP(0.45))

    # ═══════════════════════════════════════════════════════════════════
    # SECTION 4 — REPEAT CALLS
    # ═══════════════════════════════════════════════════════════════════
    story.append(sec_hdr("4", "REPEAT SERVICE CALLS",
                         f"{RC_S} / {RC_M}  —  {'PERFECT' if RC_S == 30 else 'PENALTY'}",
                         C_GREEN if RC_S == 30 else C_RED, CW))
    story.append(SP(0.15))

    rc_count = rc.get("count", 0)
    rc_calls = rc.get("calls", [])

    _rc_col = C_GREEN if RC_S == 30 else C_RED
    _rc_left = _tbl([
            [P("REPEAT CALL COUNT",  st("rh", 9, False, SILVER, TA_CENTER))],
            [P(str(rc_count),        st("rn", 50, True, _rc_col, TA_CENTER, leading=56))],
            [P("Zero Repeat Calls" if rc_count == 0 else f"{rc_count} Repeat Call(s)",
               st("rs", 10, False, _rc_col, TA_CENTER))],
        ], [CW*0.38], [
            ("TOPPADDING",(0,0),(-1,-1),0),("BOTTOMPADDING",(0,0),(-1,-1),0),
            ("LEFTPADDING",(0,0),(-1,-1),0),("RIGHTPADDING",(0,0),(-1,-1),0),
        ])
    _rc_right = _tbl([
            [P("SCORE AWARDED",      st("sh2", 9, False, SILVER))],
            [P(f"{RC_S} / {RC_M}",  st("sv", 28, True, _rc_col, leading=34))],
            [P("FULL MARKS" if RC_S == 30 else "ZERO — REPEAT RECORDED",
               st("sf", 13, True, _rc_col))],
            [SP(0.08)],
            [P("All issues resolved on first visit.\nNo repeat tickets raised this week."
               if RC_S == 30 else
               "Repeat call recorded. Score = 0/30 per policy.",
               st("sd", 10, False, INK, leading=15))],
        ], [CW*0.62], [
            ("TOPPADDING",(0,0),(-1,-1),0),("BOTTOMPADDING",(0,0),(-1,-1),0),
            ("LEFTPADDING",(0,0),(-1,-1),0),("RIGHTPADDING",(0,0),(-1,-1),0),
        ])
    rc_hero = _tbl([[_rc_left, _rc_right]], [CW*0.38, CW*0.62], [
        ("BACKGROUND",    (0,0),(-1,-1), C_GREEN_LT if RC_S == 30 else C_RED_LT),
        ("TOPPADDING",    (0,0),(-1,-1), 14),
        ("BOTTOMPADDING", (0,0),(-1,-1), 14),
        ("LEFTPADDING",   (0,0),(-1,-1), 14),
        ("RIGHTPADDING",  (0,0),(-1,-1), 14),
        ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
        ("BOX",           (0,0),(-1,-1), 1.5, C_GREEN if RC_S == 30 else C_RED),
        ("LINEAFTER",     (0,0),(0,-1),  0.7, RULE_COL),
    ])
    story.append(rc_hero)
    story.append(SP(0.12))

    if RC_S == 30:
        story.append(good("Zero repeat calls recorded — full score of 30/30 awarded as per policy.", CW))
        story.append(good("Consistent first-call resolution demonstrates strong technical "
                          "competence and reliable service quality.", CW))
    else:
        story.append(bad("Repeat call(s) recorded this week — score is 0/30 per policy.", CW))
        if rc_calls:
            rc_h = ["Company", "Machine No.", "First Visit", "Repeat Date", "Issue Type"]
            rc_data = [[P(h, st(f"rch{i}", 9, True, WHITE, TA_CENTER))
                        for i, h in enumerate(rc_h)]]
            for c in rc_calls:
                rc_data.append([
                    P(c.get("company",""),     st("rca", 10, True, C_RED)),
                    P(c.get("machine_no",""),  st("rcb", 10, False, INK, TA_CENTER)),
                    P(c.get("first_visit",""), st("rcc", 10, False, INK, TA_CENTER)),
                    P(c.get("repeat_date",""), st("rcd", 10, False, INK, TA_CENTER)),
                    P(c.get("issue_type",""),  st("rce", 10, False, INK)),
                ])
            rc_tbl = Table(rc_data,
                           colWidths=[CW*0.25, CW*0.15, CW*0.17, CW*0.17, CW*0.26],
                           repeatRows=1)
            rc_tbl.setStyle(TableStyle([
                ("BACKGROUND",    (0,0),(-1,0),  C_RED),
                ("ROWBACKGROUNDS",(0,1),(-1,-1), [WHITE, C_RED_LT]),
                ("GRID",          (0,0),(-1,-1), 0.5, RULE_COL),
                ("TOPPADDING",    (0,0),(-1,-1), 7),
                ("BOTTOMPADDING", (0,0),(-1,-1), 7),
                ("LEFTPADDING",   (0,0),(-1,-1), 8),
                ("RIGHTPADDING",  (0,0),(-1,-1), 8),
                ("VALIGN",        (0,0),(-1,-1), "MIDDLE"),
            ]))
            story.append(SP(0.1))
            story.append(rc_tbl)

    story.append(SP(0.45))

    # ── FOOTER ────────────────────────────────────────────────────────────────
    story.append(_tbl([[
        P("Tulsi Weigh Solutions Pvt. Ltd.",
          st("fl", 9, True, colors.HexColor("#8AAFC8"))),
        P("Engineer Performance Audit System",
          st("fc_", 9, False, colors.HexColor("#8AAFC8"), TA_CENTER)),
        P(f"Week  {week}  |  {rep_date}",
          st("fr", 9, False, colors.HexColor("#8AAFC8"), TA_RIGHT)),
    ]], [CW/3]*3, [
        ("BACKGROUND",    (0,0),(-1,-1), DARK),
        ("TOPPADDING",    (0,0),(-1,-1), 9),
        ("BOTTOMPADDING", (0,0),(-1,-1), 9),
        ("LEFTPADDING",   (0,0),(-1,-1), 12),
        ("RIGHTPADDING",  (0,0),(-1,-1), 12),
    ]))

    doc = SimpleDocTemplate(
        output_path, pagesize=A4,
        leftMargin=ML, rightMargin=MR,
        topMargin=1.2*cm, bottomMargin=1.2*cm,
    )
    doc.build(story)
    print(f"[Report] PDF saved: {output_path}")
    return output_path


# ╔═══════════════════════════════════════════════════════════════════╗
# ║  STEP 4 — UPLOAD & SEND VIA WHATSAPP                             ║
# ╚═══════════════════════════════════════════════════════════════════╝

def _graph():    return f"https://graph.facebook.com/{GRAPH_VERSION}"
def _wa_hdr():   return {"Authorization": f"Bearer {WA_TOKEN}"}


def upload_media_to_whatsapp(file_path: str) -> str:
    print("[WhatsApp] Uploading PDF...")
    url = f"{_graph()}/{PHONE_NUMBER_ID}/media"
    with open(file_path, "rb") as f:
        r = requests.post(
            url, headers=_wa_hdr(),
            data={"messaging_product": "whatsapp", "type": "application/pdf"},
            files={"file": (Path(file_path).name, f, "application/pdf")},
            timeout=120,
        )
    r.raise_for_status()
    mid = r.json()["id"]
    print(f"[WhatsApp] Media ID: {mid}")
    return mid


def send_whatsapp_template(media_id: str, filename: str):
    print(f"[WhatsApp] Sending to {TO_NUMBER}...")
    r = requests.post(
        f"{_graph()}/{PHONE_NUMBER_ID}/messages",
        headers={**_wa_hdr(), "Content-Type": "application/json"},
        data=json.dumps({
            "messaging_product": "whatsapp",
            "to":    TO_NUMBER,
            "type":  "template",
            "template": {
                "name":     WA_TEMPLATE_NAME,
                "language": {"code": WA_LANG},
                "components": [{
                    "type": "header",
                    "parameters": [{"type": "document",
                                    "document": {"id": media_id, "filename": filename}}],
                }],
            },
        }),
        timeout=60,
    )
    r.raise_for_status()
    print("[WhatsApp] Template message sent.")
    return r.json()


# ╔═══════════════════════════════════════════════════════════════════╗
# ║  MAIN PIPELINE                                                   ║
# ╚═══════════════════════════════════════════════════════════════════╝

def run_pipeline():
    print("=" * 55)
    print("  ENGINEER PERFORMANCE ANALYSIS PIPELINE")
    print("=" * 55)

    # Step 1 — Fetch PDF from Zoho Analytics
    pdf_bytes = fetch_zoho_pdf()

    # Step 2 — Analyse with Claude
    data = analyse_pdf_with_claude(pdf_bytes)

    # Step 3 — Generate professional A4 PDF report
    safe_name = re.sub(r"[^a-zA-Z0-9_]", "_", data.get("engineer_name", "engineer"))
    safe_week = re.sub(r"[^a-zA-Z0-9_]", "_", data.get("week_range",    "week"))
    out_path  = str(OUTPUT_DIR / f"{safe_name}_{safe_week}_report.pdf")

    generate_report_pdf(data, out_path)

    # Step 4 — Upload & send via WhatsApp
    media_id = upload_media_to_whatsapp(out_path)
    send_whatsapp_template(media_id, Path(out_path).name)

    print("=" * 55)
    print(f"  DONE — {out_path}")
    print("=" * 55)
    return out_path


if __name__ == "__main__":
    run_pipeline()