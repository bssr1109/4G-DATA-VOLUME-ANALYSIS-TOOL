from __future__ import annotations
import os
import tempfile
import pandas as pd
import streamlit as st
from streamlit.components.v1 import html as st_html

from engine import (
    TARGET_SDCAS,
    build_from_file,
    list_saved_reports,
    load_saved_csv,
    load_saved_report_html,
)

st.set_page_config(page_title="4G Daily DV Report", layout="wide")
BASE_DIR = os.path.dirname(__file__)
REPORTS_DIR = os.path.join(BASE_DIR, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

st.sidebar.title("Modules")
module = st.sidebar.radio(
    "Go to",
    [
        "Upload & Generate",
        "Today Report Viewer",
        "Historical Report Viewer",
        "Site Verification Drilldown",
        "Download Center",
    ],
)

st.sidebar.caption("Target SDCAs")
st.sidebar.write(", ".join(TARGET_SDCAS))


def render_saved(report_date: str):
    html_doc = load_saved_report_html(REPORTS_DIR, report_date)
    st_html(html_doc, height=2400, scrolling=True)


def latest_report():
    reports = list_saved_reports(REPORTS_DIR)
    return reports[0] if reports else None


def report_folder(report_date: str) -> str:
    return os.path.join(REPORTS_DIR, report_date)


def report_html_path(report_date: str) -> str:
    return os.path.join(report_folder(report_date), "report.html")


def report_png_path(report_date: str) -> str:
    return os.path.join(report_folder(report_date), "report.png")


def generate_report_image(report_date: str):
    """
    Converts saved report.html to report.png using imgkit + wkhtmltoimage.
    Returns (success: bool, message: str)
    """
    html_path = report_html_path(report_date)
    png_path = report_png_path(report_date)

    if not os.path.exists(html_path):
        return False, "report.html not found."

    try:
        import imgkit  # pip install imgkit
    except Exception:
        return False, "imgkit not installed. Run: pip install imgkit"

    try:
        options = {
            "format": "png",
            "quality": "85",
            "width": "1400",
            "enable-local-file-access": "",
        }
        imgkit.from_file(html_path, png_path, options=options)
        return True, f"Image created: {png_path}"
    except Exception as e:
        return False, (
            "Could not create image. Make sure wkhtmltoimage is installed and in PATH. "
            f"Error: {e}"
        )


def image_download_block(report_date: str):
    png_path = report_png_path(report_date)

    st.markdown("### Report Image")
    c1, c2 = st.columns([1, 2])

    with c1:
        if st.button("Generate Report Image", key=f"gen_img_{report_date}"):
            ok, msg = generate_report_image(report_date)
            if ok:
                st.success(msg)
            else:
                st.error(msg)

    with c2:
        if os.path.exists(png_path):
            with open(png_path, "rb") as f:
                st.download_button(
                    "Download Report PNG",
                    f.read(),
                    file_name=f"{report_date}_report.png",
                    mime="image/png",
                    key=f"dl_img_{report_date}",
                )
        else:
            st.info("PNG not generated yet.")


if module == "Upload & Generate":
    st.title("Upload & Generate")
    uploaded = st.file_uploader("Upload daily Excel workbook", type=["xlsx"])

    if uploaded and st.button("Generate report", type="primary"):
        with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
            tmp.write(uploaded.getbuffer())
            temp_path = tmp.name

        with open(temp_path, "rb") as f:
            parsed, master, tables, alert_text, folder = build_from_file(
                f,
                uploaded.name,
                REPORTS_DIR,
                source_file_path=temp_path,
            )

        st.success(f"Report generated and saved under {os.path.basename(folder)}")
        render_saved(parsed.analysis_date.isoformat())
        image_download_block(parsed.analysis_date.isoformat())

elif module == "Today Report Viewer":
    st.title("Today Report Viewer")
    rep = latest_report()
    if not rep:
        st.info("No saved reports yet.")
    else:
        render_saved(rep)
        image_download_block(rep)

elif module == "Historical Report Viewer":
    st.title("Historical Report Viewer")
    reports = list_saved_reports(REPORTS_DIR)
    if not reports:
        st.info("No saved reports yet.")
    else:
        selected = st.selectbox("Select analysis date", reports)
        render_saved(selected)
        image_download_block(selected)

elif module == "Site Verification Drilldown":
    st.title("Site Verification Drilldown")
    reports = list_saved_reports(REPORTS_DIR)
    if not reports:
        st.info("No saved reports yet.")
    else:
        selected = st.selectbox("Select analysis date", reports)
        df = load_saved_csv(REPORTS_DIR, selected, "site_master.csv")

        # supports either uppercase or lowercase saved columns
        site_col = "Site" if "Site" in df.columns else "site_name"
        sdca_col = "SDCA" if "SDCA" in df.columns else "sdca"

        c1, c2 = st.columns(2)
        with c1:
            sdca = st.selectbox("SDCA", ["All"] + sorted(df[sdca_col].dropna().unique().tolist()))
        filtered = df if sdca == "All" else df[df[sdca_col] == sdca]
        with c2:
            site = st.selectbox("Site", sorted(filtered[site_col].dropna().unique().tolist()))

        row = filtered[filtered[site_col] == site].iloc[0]

        # compatible keys
        today_dv = row["Today_DV"] if "Today_DV" in row else row.get("today_dv")
        dv_30_avg = row["DV_30_Avg"] if "DV_30_Avg" in row else row.get("dv_30_avg")
        dv_trend = row["DV_Trend"] if "DV_Trend" in row else row.get("dv_trend")
        today_ca = row["Today_CA"] if "Today_CA" in row else row.get("today_ca")
        ca_trend = row["CA_Trend"] if "CA_Trend" in row else row.get("ca_trend")
        status = row["Status"] if "Status" in row else row.get("issue_type", "")
        reco = row["Recommendation"] if "Recommendation" in row else row.get("recommendation", "")

        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Today DV", f"{today_dv:.2f}" if pd.notna(today_dv) else "No Data")
        m2.metric("30-Day Avg DV", "No History" if pd.isna(dv_30_avg) else f"{dv_30_avg:.2f}")
        m3.metric("DV Trend", dv_trend if pd.notna(dv_trend) else "No Data")
        m4.metric("Today CA", "No History" if pd.isna(today_ca) else f"{today_ca:.2f}")
        m5.metric("CA Trend", ca_trend if pd.notna(ca_trend) else "No Data")

        st.info(f"Status: {status}")
        if str(reco).strip():
            st.success(reco)

        st.dataframe(pd.DataFrame([row]), use_container_width=True)

elif module == "Download Center":
    st.title("Download Center")
    reports = list_saved_reports(REPORTS_DIR)
    if not reports:
        st.info("No saved reports yet.")
    else:
        selected = st.selectbox("Select analysis date", reports)
        folder = os.path.join(REPORTS_DIR, selected)

        image_download_block(selected)

        st.markdown("### Files")
        for name in sorted(os.listdir(folder)):
            path = os.path.join(folder, name)
            with open(path, "rb") as f:
                st.download_button(
                    f"Download {name}",
                    f.read(),
                    file_name=name,
                    key=f"download_{selected}_{name}",
                )