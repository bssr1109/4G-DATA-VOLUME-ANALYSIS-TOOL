
from __future__ import annotations
import json
import os
import re
import shutil
import imgkit
from dataclasses import dataclass
from datetime import datetime, date
from typing import Dict, List

import numpy as np
import pandas as pd

TARGET_SDCAS = [
    "JANGAON",
    "WARANGAL",
    "MAHABUBABAD",
    "PARKAL",
    "WARDHANNAPET",
    "MULUGU",
    "NARSAMPET",
    "ETURUNAGARAM",
    "CHERIAL",
]

SDCA_NORMALIZATION = {
    "ETURNAGARAM": "ETURUNAGARAM",
    "ETUR": "ETURUNAGARAM",
    "NARSAMPETA": "NARSAMPET",
    "MAHABUBABAD ": "MAHABUBABAD",
    "PARKAL ": "PARKAL",
}
CAT_BANDS = {
    "CAT-A": ["B28"],
    "CATB": ["B1", "B28"],
    "CAT-B": ["B1", "B28"],
    "CATC": ["B28", "B41"],
    "CAT-C": ["B28", "B41"],
    "CATD": ["B1", "B28", "B41"],
    "CAT-D": ["B1", "B28", "B41"],
}

@dataclass
class ParsedWorkbook:
    dv: pd.DataFrame
    ca: pd.DataFrame
    analysis_date: date
    source_filename: str

def _norm_sdca(v):
    if pd.isna(v):
        return ""
    t = str(v).strip().upper()
    return SDCA_NORMALIZATION.get(t, t)

def _norm_cat(v):
    if pd.isna(v):
        return ""
    t = str(v).strip().upper().replace(" ", "")
    if t in ("CATA", "CAT-A"): return "CAT-A"
    if t in ("CATB", "CAT-B"): return "CAT-B"
    if t in ("CATC", "CAT-C"): return "CAT-C"
    if t in ("CATD", "CAT-D"): return "CAT-D"
    return t

def slugify(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")

def _flatten(mi):
    out = []
    for a, b in mi:
        a = "" if pd.isna(a) else str(a).strip()
        b = "" if pd.isna(b) else str(b).strip()
        out.append(f"{a} | {b}" if a and b else (b or a))
    return out

def _date_from_col(col: str) -> str | None:
    m = re.match(r"^(\d{4}-\d{2}-\d{2}) 00:00:00 \| ", col)
    return m.group(1) if m else None

def parse_workbook(file_obj, source_filename: str) -> ParsedWorkbook:
    dv = pd.read_excel(file_obj, sheet_name="Data Volume", header=[0,1])
    if hasattr(file_obj, "seek"):
        file_obj.seek(0)
    ca = pd.read_excel(file_obj, sheet_name="Cell Availability", header=[0,1])

    dv.columns = _flatten(dv.columns)
    ca.columns = _flatten(ca.columns)

    dv = dv.rename(columns={
        [c for c in dv.columns if c.endswith("| Site Name")][0]: "Site",
        [c for c in dv.columns if c.endswith("| SDCA")][0]: "SDCA",
        [c for c in dv.columns if c.endswith("| RP ID")][0]: "RP ID",
        [c for c in dv.columns if c.endswith("| Installed CAT")][0]: "Installed CAT",
    })
    ca = ca.rename(columns={
        [c for c in ca.columns if c.endswith("| Site Name")][0]: "Site",
        [c for c in ca.columns if c.endswith("| SDCA")][0]: "SDCA",
    })

    dv["SDCA"] = dv["SDCA"].map(_norm_sdca)
    ca["SDCA"] = ca["SDCA"].map(_norm_sdca)
    dv["Installed CAT"] = dv["Installed CAT"].map(_norm_cat)

    dv = dv[dv["SDCA"].isin(TARGET_SDCAS)].copy()
    ca = ca[ca["SDCA"].isin(TARGET_SDCAS)].copy()

    dv_date_cols = [c for c in dv.columns if c.endswith("| Total DV per day in GB") and _date_from_col(c)]
    dv_date_cols = sorted(dv_date_cols, key=lambda c: _date_from_col(c), reverse=True)
    if not dv_date_cols:
        raise ValueError("No DV date columns found.")
    analysis_date = datetime.fromisoformat(_date_from_col(dv_date_cols[0])).date()

    return ParsedWorkbook(dv=dv, ca=ca, analysis_date=analysis_date, source_filename=source_filename)

def _numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def _sector_cols_for_band(df: pd.DataFrame, band: str) -> List[str]:
    prefix = {"B1":"B1-2100 | B1-Sector-", "B28":"B28-700 | B28-Sector-", "B41":"B41-2500 | B41-Sector-"}[band]
    return [c for c in df.columns if c.startswith(prefix)]

def _band_total_col(df: pd.DataFrame, band: str) -> str | None:
    name = {"B1":"B1-2100 | B1 Total", "B28":"B28-700 | B28 Total", "B41":"B41-2500 | B41 Total"}[band]
    return name if name in df.columns else None

def _sector_issue(vals: List[float], band: str) -> str:
    vals = [0.0 if pd.isna(v) else float(v) for v in vals]
    zero = [f"S{i}" for i,v in enumerate(vals) if v == 0]
    if zero:
        return f"Zero Sector: {','.join(zero)} ({band})"
    if all(v > 0 for v in vals):
        mx = max(vals)
        low = [f"S{i}" for i,v in enumerate(vals) if v < 0.2 * mx]
        if low:
            return f"Low Sector: {','.join(low)} ({band})"
    return ""

def _issue_for_row(row: pd.Series) -> tuple[str,str,str,str]:
    cat = row.get("Installed CAT","")
    applicable = CAT_BANDS.get(cat, ["B1","B28","B41"])
    issues = []
    band_status = {}
    for band in ["B1","B28","B41"]:
        if band not in applicable:
            band_status[band] = "Not Applicable"
            continue
        cols = [f"{band}_S0", f"{band}_S1", f"{band}_S2"]
        issue = _sector_issue([row.get(c) for c in cols], band)
        band_status[band] = issue or "Healthy"
        if issue:
            issues.append(issue)
    overall = "Healthy" if not issues else " | ".join(issues)
    return overall, band_status["B1"], band_status["B28"], band_status["B41"]

def prepare_site_master(parsed: ParsedWorkbook) -> pd.DataFrame:
    dv = parsed.dv.copy()
    ca = parsed.ca.copy()

    # rename sector and total cols
    rename_map = {}
    for band in ["B1","B28","B41"]:
        scols = _sector_cols_for_band(dv, band)
        for i,c in enumerate(sorted(scols)):
            rename_map[c] = f"{band}_S{i}"
        total = _band_total_col(dv, band)
        if total:
            rename_map[total] = f"{band}_Total"
    dv = dv.rename(columns=rename_map)

    # dv columns
    dv_cols = [c for c in dv.columns if c.endswith("| Total DV per day in GB") and _date_from_col(c)]
    dv_cols = sorted(dv_cols, key=lambda c: _date_from_col(c), reverse=True)
    dv = _numeric(dv, list(rename_map.values()) + dv_cols)

    dv["Today_DV"] = dv[dv_cols[0]]
    prev_dv = dv_cols[1:31]
    dv["DV_30_Avg"] = dv[prev_dv].mean(axis=1) if prev_dv else np.nan
    dv["DV_Trend"] = np.where(dv["Today_DV"] > dv["DV_30_Avg"], "Increase", np.where(dv["Today_DV"] < dv["DV_30_Avg"], "Decrease", "Flat"))

    # ca
    ca_cols = [c for c in ca.columns if c.endswith("| Total CA per day") and _date_from_col(c)]
    ca_cols = sorted(ca_cols, key=lambda c: _date_from_col(c), reverse=True)
    ca = _numeric(ca, ca_cols)
    if ca_cols:
        ca["Today_CA"] = ca[ca_cols[0]]
        prev_ca = ca_cols[1:31]
        ca["CA_30_Avg"] = ca[prev_ca].mean(axis=1) if prev_ca else np.nan
        ca["CA_Trend"] = np.where(ca["Today_CA"] > ca["CA_30_Avg"], "Increase", np.where(ca["Today_CA"] < ca["CA_30_Avg"], "Decrease", "Flat"))
    else:
        ca["Today_CA"] = np.nan
        ca["CA_30_Avg"] = np.nan
        ca["CA_Trend"] = "No History"

    ca_small = ca[["Site","Today_CA","CA_30_Avg","CA_Trend"]].copy()
    m = dv.merge(ca_small, on="Site", how="left")

    issue_info = m.apply(_issue_for_row, axis=1, result_type="expand")
    issue_info.columns = ["Status","B1_Status","B28_Status","B41_Status"]
    m = pd.concat([m, issue_info], axis=1)

    # marketing recommendation
    m["Recommendation"] = np.where(
        (m["Today_CA"].fillna(0) >= 95)
        & (m["DV_Trend"] == "Decrease"),
        "Advise Marketing Campaign",
        "",
    )

    # clean display cols
    keep = [
        "Site","SDCA","Installed CAT","Today_DV","DV_30_Avg","DV_Trend",
        "Today_CA","CA_30_Avg","CA_Trend","Status","B1_Status","B28_Status","B41_Status","Recommendation"
    ]
    return m[keep].copy()

def build_report_tables(master: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    top_sites = master.sort_values("Today_DV", ascending=False).head(10).copy()
    worst_sites = master.sort_values(["Today_DV"], ascending=[True]).head(15).copy()
    used = set(top_sites.index).union(set(worst_sites.index))
    remaining = master.loc[~master.index.isin(used)].sort_values(["SDCA","Today_DV"], ascending=[True, False]).copy()

    sdca_rows = []
    for sdca in TARGET_SDCAS:
        sub = remaining[remaining["SDCA"] == sdca].copy()
        dv_sum = sub["Today_DV"].sum()
        avg_sum = sub["DV_30_Avg"].sum()
        trend = "Increase" if dv_sum > avg_sum else ("Decrease" if dv_sum < avg_sum else "Flat")
        sdca_rows.append({
            "SDCA": sdca,
            "Remaining Sites": len(sub),
            "Today DV": dv_sum,
            "30-Day Avg DV": avg_sum,
            "Trend": trend,
            "Zero Sector": sub["Status"].astype(str).str.contains("Zero Sector").sum(),
            "Low Sector": sub["Status"].astype(str).str.contains("Low Sector").sum(),
            "Marketing Recommended": sub["Recommendation"].astype(str).str.contains("Marketing").sum(),
        })
    sdca_summary = pd.DataFrame(sdca_rows)
    return {"top_sites": top_sites, "worst_sites": worst_sites, "remaining_sites": remaining, "sdca_summary": sdca_summary}

def generate_combined_alert(master: pd.DataFrame) -> str:
    lines = ["WARANGAL OA - 4G Daily DV Alert", ""]
    for sdca in TARGET_SDCAS:
        sub = master[master["SDCA"] == sdca]
        zero = sub[sub["Status"].astype(str).str.contains("Zero Sector")]
        low = sub[sub["Status"].astype(str).str.contains("Low Sector")]
        mkt = sub[sub["Recommendation"].astype(str).str.contains("Marketing")]
        lines.append(sdca)
        lines.append(f"- Total Sites Analysed: {len(sub)}")
        lines.append(f"- Today DV: {sub['Today_DV'].sum():.2f} GB")
        if not zero.empty:
            lines.append(f"- Zero Sector Sites: {len(zero)}")
        if not low.empty:
            lines.append(f"- Low Sector Sites: {len(low)}")
        if not mkt.empty:
            lines.append(f"- Advise Marketing Campaign: {len(mkt)}")
        if zero.empty and low.empty and mkt.empty:
            lines.append("- No major anomalies")
        lines.append("")
    return "\n".join(lines).strip()

def _fmt_num(x):
    return "No History" if pd.isna(x) else f"{float(x):,.2f}"

def _badge(text: str) -> str:
    t = str(text)
    if not t:
        return ""
    if t == "Healthy":
        color = "#15803d"
    elif "Zero Sector" in t:
        color = "#b91c1c"
    elif "Low Sector" in t:
        color = "#c2410c"
    elif "Marketing" in t:
        color = "#166534"
    else:
        color = "#475569"
    return f'<span style="background:{color};color:white;padding:4px 8px;border-radius:999px;font-size:12px;">{t}</span>'

def _row_html(r: pd.Series) -> str:
    return (
        f"<tr><td>{r['Site']}</td><td>{r['SDCA']}</td><td>{r['Installed CAT']}</td>"
        f"<td>{_fmt_num(r['Today_DV'])}</td><td>{_fmt_num(r['DV_30_Avg'])}</td><td>{r['DV_Trend']}</td>"
        f"<td>{_fmt_num(r['Today_CA'])}</td><td>{_fmt_num(r['CA_30_Avg'])}</td><td>{r['CA_Trend']}</td>"
        f"<td>{_badge(r['Status'])}</td><td>{r['B1_Status']}</td><td>{r['B28_Status']}</td><td>{r['B41_Status']}</td><td>{r['Recommendation']}</td></tr>"
    )

def render_html_report(parsed: ParsedWorkbook, master: pd.DataFrame, tables: Dict[str, pd.DataFrame], alert_text: str) -> str:
    total_today = master["Today_DV"].sum()
    total_avg = master["DV_30_Avg"].sum()
    dv_delta = total_today - total_avg
    dv_label = "Increase" if dv_delta > 0.01 else ("Decrease" if dv_delta < -0.01 else "Flat")
    ca_today = master["Today_CA"].mean()
    ca_avg = master["CA_30_Avg"].mean()
    ca_delta = ca_today - ca_avg if pd.notna(ca_today) and pd.notna(ca_avg) else np.nan
    ca_label = "Increase" if pd.notna(ca_delta) and ca_delta > 0.01 else ("Decrease" if pd.notna(ca_delta) and ca_delta < -0.01 else "Flat")

    top_rows = ''.join(_row_html(r) for _, r in tables["top_sites"].iterrows())
    worst_rows = ''.join(_row_html(r) for _, r in tables["worst_sites"].iterrows())

    sdca_rows = []
    sdca_dropdowns = []
    for _, r in tables["sdca_summary"].iterrows():
        sdca = r["SDCA"]
        subset = tables["remaining_sites"][tables["remaining_sites"]["SDCA"] == sdca]
        sdca_rows.append(
            f"<tr><td><a href='#sdca_{slugify(sdca)}'>{sdca}</a></td><td>{int(r['Remaining Sites'])}</td><td>{_fmt_num(r['Today DV'])}</td><td>{_fmt_num(r['30-Day Avg DV'])}</td><td>{r['Trend']}</td><td>{int(r['Zero Sector'])}</td><td>{int(r['Low Sector'])}</td><td>{int(r['Marketing Recommended'])}</td></tr>"
        )
        rows = ''.join(_row_html(rr) for _, rr in subset.iterrows())
        sdca_dropdowns.append(
            f"<details class='drop' id='sdca_{slugify(sdca)}'><summary>{sdca} - Remaining Sites</summary>"
            f"<table><tr><th>Site</th><th>SDCA</th><th>Installed CAT</th><th>Today DV</th><th>30-Day Avg DV</th><th>DV Trend</th><th>Today CA</th><th>30-Day Avg CA</th><th>CA Trend</th><th>Status</th><th>B1</th><th>B28</th><th>B41</th><th>Recommendation</th></tr>{rows}</table></details>"
        )

    analysis_str = parsed.analysis_date.strftime("%d-%b-%Y")
    generated_on = datetime.now().strftime("%d-%b-%Y %H:%M:%S")

    return f"""
    <html><head><meta charset='utf-8'/>
    <style>
    body {{ font-family: Arial, sans-serif; background:#f5f7fb; color:#0f172a; margin:0; }}
    .wrap {{ max-width: 1500px; margin:0 auto; padding:24px; }}
    .hero {{ background:#142c6e; color:white; padding:28px 32px; border-radius:18px; }}
    .hero h1 {{ margin:0 0 10px 0; font-size:30px; }}
    .sub {{ font-size:16px; line-height:1.7; }}
    .grid {{ display:grid; grid-template-columns: repeat(5, 1fr); gap:16px; margin:20px 0; }}
    .card {{ background:white; border-radius:16px; padding:18px; box-shadow:0 2px 12px rgba(15,23,42,0.08); }}
    .metric {{ font-size:28px; font-weight:700; margin-top:8px; }}
    h2 {{ margin-top:28px; }}
    table {{ width:100%; border-collapse:collapse; background:white; border-radius:14px; overflow:hidden; }}
    th, td {{ padding:10px 12px; border-bottom:1px solid #e5e7eb; text-align:left; font-size:14px; vertical-align:top; }}
    th {{ background:#e2e8f0; }}
    tr:nth-child(even) td {{ background:#f8fafc; }}
    .alert {{ background:#fff7ed; border-left:5px solid #ea580c; padding:16px; white-space:pre-wrap; border-radius:12px; }}
    .drop {{ margin:16px 0; background:white; border-radius:14px; box-shadow:0 2px 12px rgba(15,23,42,0.08); overflow:hidden; }}
    .drop summary {{ cursor:pointer; padding:14px 16px; font-weight:700; background:#e2e8f0; }}
    </style></head><body>
    <div class='wrap'>
      <div class='hero'>
        <h1>4G Daily DV Report</h1>
        <div class='sub'>
          <div><strong>Analysis Date:</strong> {analysis_str}</div>
          <div><strong>Source File:</strong> {parsed.source_filename}</div>
          <div><strong>Generated On:</strong> {generated_on}</div>
          <div><strong>SDCAs:</strong> {', '.join(TARGET_SDCAS)}</div>
        </div>
      </div>

      <div class='grid'>
        <div class='card'><div>Today Total DV</div><div class='metric'>{_fmt_num(total_today)}</div></div>
        <div class='card'><div>30-Day Avg DV</div><div class='metric'>{_fmt_num(total_avg)}</div></div>
        <div class='card'><div>DV vs 30-Day Avg</div><div class='metric'>{_fmt_num(dv_delta)}</div><div>{dv_label}</div></div>
        <div class='card'><div>Today Avg Availability</div><div class='metric'>{_fmt_num(ca_today)}</div></div>
        <div class='card'><div>Availability vs 30-Day Avg</div><div class='metric'>{_fmt_num(ca_delta)}</div><div>{ca_label}</div></div>
      </div>

      <h2>Top DV Sites</h2>
      <table><tr><th>Site</th><th>SDCA</th><th>Installed CAT</th><th>Today DV</th><th>30-Day Avg DV</th><th>DV Trend</th><th>Today CA</th><th>30-Day Avg CA</th><th>CA Trend</th><th>Status</th><th>B1</th><th>B28</th><th>B41</th><th>Recommendation</th></tr>{top_rows}</table>

      <h2>Worst / Priority Sites</h2>
      <table><tr><th>Site</th><th>SDCA</th><th>Installed CAT</th><th>Today DV</th><th>30-Day Avg DV</th><th>DV Trend</th><th>Today CA</th><th>30-Day Avg CA</th><th>CA Trend</th><th>Status</th><th>B1</th><th>B28</th><th>B41</th><th>Recommendation</th></tr>{worst_rows}</table>

      <h2>Remaining Sites - SDCA Wise</h2>
      <table><tr><th>SDCA</th><th>Remaining Sites</th><th>Today DV</th><th>30-Day Avg DV</th><th>Trend</th><th>Zero Sector</th><th>Low Sector</th><th>Marketing Recommended</th></tr>{''.join(sdca_rows)}</table>
      {''.join(sdca_dropdowns)}

      <h2>Combined Alert</h2>
      <div class='alert'>{alert_text}</div>
    </div></body></html>
    """

def save_report_bundle(parsed: ParsedWorkbook, master: pd.DataFrame, tables: Dict[str, pd.DataFrame], alert_text: str, reports_dir: str, source_file_path: str | None = None) -> str:
    report_dir = os.path.join(reports_dir, parsed.analysis_date.isoformat())
    os.makedirs(report_dir, exist_ok=True)

    html_doc = render_html_report(parsed, master, tables, alert_text)
    with open(os.path.join(report_dir, "report.html"), "w", encoding="utf-8") as f:
        f.write(html_doc)

    master.to_csv(os.path.join(report_dir, "site_master.csv"), index=False)
    for name, df in tables.items():
        df.to_csv(os.path.join(report_dir, f"{name}.csv"), index=False)
    with open(os.path.join(report_dir, "field_alerts.txt"), "w", encoding="utf-8") as f:
        f.write(alert_text)
    meta = {
        "analysis_date": parsed.analysis_date.isoformat(),
        "source_filename": parsed.source_filename,
        "generated_on": datetime.now().isoformat(timespec="seconds"),
        "sites_analysed": int(len(master)),
        "sdcas": TARGET_SDCAS,
    }
    with open(os.path.join(report_dir, "metadata.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)
    if source_file_path and os.path.exists(source_file_path):
        shutil.copy2(source_file_path, os.path.join(report_dir, os.path.basename(source_file_path)))
    return report_dir

def list_saved_reports(reports_dir: str):
    if not os.path.exists(reports_dir):
        return []
    return sorted([d for d in os.listdir(reports_dir) if os.path.isdir(os.path.join(reports_dir, d))], reverse=True)

def load_saved_report_html(reports_dir: str, report_date: str) -> str:
    with open(os.path.join(reports_dir, report_date, "report.html"), "r", encoding="utf-8") as f:
        return f.read()

def load_saved_csv(reports_dir: str, report_date: str, name: str) -> pd.DataFrame:
    return pd.read_csv(os.path.join(reports_dir, report_date, name))

def build_from_file(file_obj, source_filename: str, reports_dir: str, source_file_path: str | None = None):
    parsed = parse_workbook(file_obj, source_filename)
    master = prepare_site_master(parsed)
    tables = build_report_tables(master)
    alert_text = generate_combined_alert(master)
    folder = save_report_bundle(parsed, master, tables, alert_text, reports_dir, source_file_path)
    return parsed, master, tables, alert_text, folder

def save_report_image(html_path, output_path):
    options = {
        'format': 'png',
        'quality': '80',
        'width': '1400'
    }
    imgkit.from_file(html_path, output_path, options=options)
