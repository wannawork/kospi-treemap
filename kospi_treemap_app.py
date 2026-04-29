"""
KOSPI 시가총액 트리맵 대시보드 (업데이트 버전)
- 필터링된 조건 기반 2년 내 최대 시총 가이드라인(점선) 추가
"""

import os, json, time
from datetime import datetime, timedelta

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import requests

st.set_page_config(
    page_title="KOSPI 시가총액 트리맵",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ═══════════════════════════════════════
# CONFIG
# ═══════════════════════════════════════
APP_KEY    = "PSGQHeNH22lAI4BmtTI2eYvuqYiSr930YRtu"
APP_SECRET = ("VRNSXejM5BOCV/rdDOZWSwxr6pmsUniHFSyv08ny7TrWQkW4NJCjKjv4RhmvKKbn"
              "uIi43QbjwuF+R1Ekd/ppvDCIbC+iFc3GF7EV+C+8Q86eP3PzwqWYWxgrceuG/yIV0"
              "zsgHJLYFHP1yNGXRAMz0XK3znP6+uGGmfuINp8Orm/wVFSiaUg=")
BASE_URL         = "https://openapi.koreainvestment.com:9443"
SPREADSHEET_ID   = "1AKdxk-5_nKsf7smFGOCek4CWgxNz8b4yBk89hIQGMLk"
CALENDAR_ID      = "1LiRq6Fvs8wjI_HwhyxBdFTcwtifyKiz1rOzqOul5gDY"
LOCAL_CSV        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kospi_data.csv")
KEEP_DAYS        = 730   
SCALE_TIERS      = [2000,3000,4000,5000,6000,8000,10000]
COLS             = ["Date","Code","Name","Sector","Marcap","Price","Rank"]

# ═══════════════════════════════════════
# 한투 API 및 데이터 처리 함수 (기존과 동일)
# ═══════════════════════════════════════

@st.cache_data(ttl=3300)
def get_access_token():
    for attempt in range(3):
        try:
            res = requests.post(f"{BASE_URL}/oauth2/tokenP", json={
                "grant_type": "client_credentials",
                "appkey": APP_KEY, "appsecret": APP_SECRET
            }, timeout=15)
            if res.status_code == 200:
                return res.json().get("access_token")
        except Exception:
            pass
        if attempt < 2:
            time.sleep(62)
    return None

def make_headers(token, tr_id):
    return {
        "content-type":  "application/json",
        "authorization": f"Bearer {token}",
        "appkey":        APP_KEY,
        "appsecret":     APP_SECRET,
        "tr_id":         tr_id,
        "custtype":      "P",
    }

@st.cache_data(ttl=300, show_spinner=False)
def fetch_top_stocks(token):
    url = f"{BASE_URL}/uapi/domestic-stock/v1/ranking/market-cap"
    params = {
        "fid_cond_mrkt_div_code":  "J",
        "fid_cond_scr_div_code":   "20174",
        "fid_div_cls_code":        "0",
        "fid_input_iscd":          "0001",
        "fid_trgt_cls_code":       "0",
        "fid_trgt_exls_cls_code":  "0",
    }
    try:
        res  = requests.get(url, headers=make_headers(token, "FHPST01740000"), params=params, timeout=10)
        data = res.json()
        if data.get("rt_cd") == "0" and data.get("output"):
            return [{"Code": r.get("mksc_shrn_iscd",""), "Name": r.get("hts_kor_isnm",""), "Rank": int(r.get("data_rank",0))} for r in data["output"]]
    except Exception:
        return []
    return []

def fetch_sector_hts(token, code):
    try:
        res  = requests.get(
            f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=make_headers(token, "FHKST01010100"),
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
            timeout=5
        )
        return res.json().get("output", {}).get("bstp_kor_isnm", "기타")
    except Exception:
        return "기타"

def fetch_stock_history(token, code, start, end):
    try:
        res = requests.get(
            f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-daily-itemchartprice",
            headers=make_headers(token, "FHKST03010100"),
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD":         code,
                "FID_INPUT_DATE_1":       start,
                "FID_INPUT_DATE_2":       end,
                "FID_PERIOD_DIV_CODE":    "D",
                "FID_ORG_ADJ_PRC":        "0",
            },
            timeout=10
        )
        data = res.json()
        if data.get("rt_cd") != "0": return []
        lstn_stcn = int(data["output1"].get("lstn_stcn", 0))
        if lstn_stcn == 0: return []
        return [{"Date": r["stck_bsop_date"], "Price": float(r["stck_clpr"]), "Marcap": int(float(r["stck_clpr"]) * lstn_stcn)} for r in data.get("output2", [])]
    except Exception:
        return []

@st.cache_data(ttl=600, show_spinner=False)
def get_kospi_index(token):
    try:
        res  = requests.get(
            f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-index-category-price",
            headers=make_headers(token, "FHPUP02140000"),
            params={"FID_COND_MRKT_DIV_CODE":"U","FID_INPUT_ISCD":"0001","FID_COND_SCR_DIV_CODE":"20214","FID_MRKT_CLS_CODE":"K","FID_BLNG_CLS_CODE":"0"},
            timeout=10
        )
        return float(res.json().get("output1", {}).get("bstp_nmix_prpr", 0))
    except Exception:
        return 0.0

def fetch_history_bulk(token, top_stocks, start, end, label):
    total   = len(top_stocks)
    bar     = st.progress(0, text=f"{label} 시작...")
    sec_map = {s["Code"]: fetch_sector_hts(token, s["Code"]) for s in top_stocks}
    
    records = []
    for i, s in enumerate(top_stocks):
        code, name, rank = s["Code"], s["Name"], s["Rank"]
        rows = fetch_stock_history(token, code, start, end)
        for r in rows:
            records.append({
                "Date": pd.to_datetime(r["Date"]), "Code": code, "Name": name,
                "Sector": sec_map.get(code, "기타"), "Marcap": r["Marcap"], "Price": r["Price"], "Rank": rank
            })
        bar.progress((i+1)/total)
    bar.empty()
    return pd.DataFrame(records)

# ═══════════════════════════════════════
# DATA I/O (기존 로직 유지)
# ═══════════════════════════════════════

def _try_sheets_client():
    try:
        from google.oauth2.service_account import Credentials
        import gspread
        raw = st.secrets.get("GCP_SERVICE_ACCOUNT", "")
        if not raw: return None, None
        info = json.loads(raw)
        creds = Credentials.from_service_account_info(info, scopes=["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"])
        return gspread.authorize(creds), info
    except: return None, None

def load_data():
    gc, info = _try_sheets_client()
    if gc:
        try:
            ss = gc.open_by_key(SPREADSHEET_ID)
            ws = ss.sheet1
            df = pd.DataFrame(ws.get_all_records())
            if not df.empty:
                df["Date"] = pd.to_datetime(df["Date"])
                return df, "sheets"
        except: pass
    if os.path.exists(LOCAL_CSV):
        df = pd.read_csv(LOCAL_CSV)
        df["Date"] = pd.to_datetime(df["Date"])
        return df, "local"
    return pd.DataFrame(columns=COLS), "empty"

def save_data(new_df):
    new_df.to_csv(LOCAL_CSV, index=False)
    gc, info = _try_sheets_client()
    if gc:
        try:
            ss = gc.open_by_key(SPREADSHEET_ID)
            ws = ss.sheet1
            existing = pd.DataFrame(ws.get_all_records())
            combined = pd.concat([existing, new_df]).drop_duplicates(subset=["Date","Code"])
            ws.update([combined.columns.values.tolist()] + combined.values.tolist())
        except: pass

def load_events():
    gc, _ = _try_sheets_client()
    if not gc: return pd.DataFrame()
    try:
        ss = gc.open_by_key(CALENDAR_ID)
        df = pd.DataFrame(ss.sheet1.get_all_records())
        df["Date"] = pd.to_datetime(df["Date"])
        return df
    except: return pd.DataFrame()

# ═══════════════════════════════════════
# 시각화 (수정됨: filtered_max 반영)
# ═══════════════════════════════════════

def get_scale_info(total_t):
    for i, cap in enumerate(SCALE_TIERS):
        if total_t <= cap:
            lo = SCALE_TIERS[i-1] if i > 0 else 0
            return f"{lo:,}조 ~ {cap:,}조", cap
    return f"{SCALE_TIERS[-1]:,}조 이상", SCALE_TIERS[-1] + 2000

def build_treemap(df, date_str, kospi_idx, total_min, total_max, filtered_max):
    """
    filtered_max: 현재 필터(섹터/종목 제외)가 적용된 상태에서의 2년 내 최대 시총
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="데이터 없음", x=0.5, y=0.5, showarrow=False)
        return fig

    total_t = df["Marcap"].sum() / 1e12
    
    # 박스 크기 비율 계산 (현재 필터링된 최대치 대비 비율)
    if filtered_max > 0:
        raw_ratio = total_t / filtered_max
        scale_ratio = max(raw_ratio, 0.5) # 최소 높이 50% 보장
    else:
        raw_ratio = 1.0
        scale_ratio = 1.0

    chart_height = max(int(560 * scale_ratio), 300)

    sec_sum = df.groupby("Sector")["Marcap"].sum()
    df["Sector_Label"] = df["Sector"].apply(lambda s: f"{s} ({sec_sum.get(s,0)/1e12:.0f}조)")
    
    def get_color(name):
        if "삼성전자" in name and "우" not in name: return "#FFD700"
        if "SK하이닉스" in name: return "#FF8C00"
        return "#E5ECF6"
    df["_c"] = df["Name"].apply(get_color)

    import plotly.express as px
    fig = px.treemap(
        df, path=["Sector_Label", "Name"], values="Marcap",
        color="_c", color_discrete_map={c: c for c in df["_c"].unique()},
        custom_data=["Sector", "Price", "Rank", "Marcap"]
    )

    # ── 가이드라인(점선) 추가 ──
    # 필터링된 상태의 최대 크기(100%)를 나타내는 점선 박스
    fig.add_shape(
        type="rect", x0=0, y0=0, x1=1, y1=1, xref="paper", yref="paper",
        line=dict(color="rgba(0,0,0,0.3)", width=2, dash="dot")
    )
    fig.add_annotation(
        x=1, y=1.02, xref="paper", yref="paper",
        text=f"현재 조건 내 2년 최대치 ({filtered_max:,.0f}조) 기준선",
        showarrow=False, font=dict(size=10, color="gray"), xanchor="right"
    )

    fig.update_layout(
        title=dict(
            text=f"📈 KOSPI {kospi_idx:,.2f} | {date_str} | 현재 {total_t:,.0f}조 (최대 대비 {raw_ratio*100:.1f}%)",
            x=0.5, font=dict(size=13)
        ),
        margin=dict(t=55, l=5, r=120, b=5),
        height=chart_height,
        uniformtext=dict(minsize=9, mode="hide")
    )
    return fig

def build_trend_chart(df_all, selected_date, df_events=None):
    daily = df_all.groupby("Date")["Marcap"].sum().reset_index()
    daily["조"] = daily["Marcap"] / 1e12
    fig = go.Figure(go.Scatter(x=daily["Date"], y=daily["조"], fill="tozeroy", mode="lines", line=dict(color="#1565C0")))
    
    sel = daily[daily["Date"].dt.date == selected_date]
    if not sel.empty:
        fig.add_trace(go.Scatter(x=sel["Date"], y=sel["조"], mode="markers", marker=dict(color="red", size=10)))

    fig.update_layout(height=200, margin=dict(t=20, b=20, l=40, r=40), showlegend=False)
    return fig

# ═══════════════════════════════════════
# MAIN (동적 필터링 로직 포함)
# ═══════════════════════════════════════

def main():
    st.title("📈 KOSPI 시가총액 트리맵")

    if "token" not in st.session_state:
        st.session_state["token"] = get_access_token()
    token = st.session_state["token"]

    df_history, source = load_data()
    if df_history.empty:
        st.info("데이터 수집 중...")
        top_stocks = fetch_top_stocks(token)
        df_history = fetch_history_bulk(token, top_stocks, (datetime.now()-timedelta(days=730)).strftime("%Y%m%d"), datetime.now().strftime("%Y%m%d"), "초기 수집")
        save_data(df_history)

    available_dates = sorted(df_history["Date"].dt.date.unique())
    
    # ── 사이드바 필터 ──
    with st.sidebar:
        st.header("🔍 필터")
        all_sectors = sorted(df_history["Sector"].unique())
        sector_mode = st.radio("섹터 필터", ["전체", "선택 섹터만", "특정 섹터 제외"])
        
        sel_sectors = all_sectors
        if sector_mode == "선택 섹터만":
            sel_sectors = st.multiselect("섹터 선택", all_sectors, default=all_sectors)
        elif sector_mode == "특정 섹터 제외":
            excl = st.multiselect("제외 섹터", all_sectors)
            sel_sectors = [s for s in all_sectors if s not in excl]

        excl_stocks = st.multiselect("제외 종목", sorted(df_history["Name"].unique()))
        top_n = st.slider("종목 수", 30, 200, 100)

    # ── 필터링된 히스토리 및 최대값 계산 ──
    # 사용자가 제외한 섹터/종목을 히스토리 전체에 적용하여 '역대 최대'를 구함
    df_hist_filtered = df_history[df_history["Sector"].isin(sel_sectors)].copy()
    df_hist_filtered = df_hist_filtered[~df_hist_filtered["Name"].isin(excl_stocks)]
    
    if not df_hist_filtered.empty:
        daily_filtered_totals = df_hist_filtered.groupby("Date")["Marcap"].sum() / 1e12
        filtered_max = daily_filtered_totals.max()
        total_min = daily_filtered_totals.min()
        total_max = daily_filtered_totals.max() # 전역 최대값
    else:
        filtered_max = 1.0; total_min = 0; total_max = 1.0

    # ── 날짜 선택 및 당일 데이터 ──
    if "date_idx" not in st.session_state: st.session_state["date_idx"] = len(available_dates)-1
    selected_date = available_dates[st.session_state["date_idx"]]

    df_day = df_hist_filtered[df_hist_filtered["Date"].dt.date == selected_date].copy()
    df_day = df_day.sort_values("Marcap", ascending=False).head(top_n)

    # ── 화면 출력 ──
    kospi_idx = get_kospi_index(token)
    st.plotly_chart(build_treemap(df_day, str(selected_date), kospi_idx, total_min, total_max, filtered_max), use_container_width=True)

    # 날짜 조절
    c1, c2 = st.columns(2)
    if c1.button("◀ 이전날") : st.session_state["date_idx"] = max(0, st.session_state["date_idx"]-1); st.rerun()
    if c2.button("다음날 ▶") : st.session_state["date_idx"] = min(len(available_dates)-1, st.session_state["date_idx"]+1); st.rerun()

    st.plotly_chart(build_trend_chart(df_history, selected_date), use_container_width=True)

if __name__ == "__main__":
    main()