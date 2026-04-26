"""
KOSPI 시가총액 트리맵 대시보드
- 한투 API + HTS 업종 분류
- Google Drive 2년치 데이터 저장
- 트리맵 하단 날짜 슬라이더 + 시총 비례 박스 크기
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
SPREADSHEET_NAME = "KOSPI_MarketCap"
SPREADSHEET_ID   = "1AKdxk-5_nKsf7smFGOCek4CWgxNz8b4yBk89hIQGMLk"
CALENDAR_ID      = "1LiRq6Fvs8wjI_HwhyxBdFTcwtifyKiz1rOzqOul5gDY"
PARENT_FOLDER    = "개인"
SUB_FOLDER       = "stock_data_2"
COLS             = ["Date","Code","Name","Sector","Marcap","Price","Rank"]
LOCAL_CSV        = os.path.join(os.path.dirname(os.path.abspath(__file__)), "kospi_data.csv")
KEEP_DAYS        = 730   # 2년
SCALE_TIERS      = [2000,3000,4000,5000,6000,8000,10000]

# ═══════════════════════════════════════
# KRX 표준산업 → HTS 업종명 매핑
# ═══════════════════════════════════════
KRX_TO_HTS = {
    # 전기·전자
    "반도체 및 반도체장비":         "전기·전자",
    "전자장비 및 기기":              "전기·전자",
    "전자제품":                      "전기·전자",
    "디스플레이":                    "전기·전자",
    "IT하드웨어":                    "전기·전자",
    # 운송장비·부품
    "자동차":                        "운송장비·부품",
    "자동차부품":                    "운송장비·부품",
    "조선":                          "운송장비·부품",
    "항공기 및 항공기부품":          "운송장비·부품",
    # 화학
    "화학":                          "화학",
    "정유":                          "화학",
    "석유화학":                      "화학",
    "합성수지":                      "화학",
    # 금융
    "상업은행":                      "금융",
    "증권":                          "증권",
    "보험":                          "보험",
    "기타금융":                      "금융",
    "지주회사":                      "금융",
    "자산관리":                      "금융",
    # 제약
    "제약":                          "제약",
    "바이오":                        "제약",
    "의료기기":                      "의료·정밀기기",
    "의료서비스":                    "의료·정밀기기",
    # 건설
    "건설":                          "건설",
    "건축자재":                      "건설",
    # 운송·창고
    "해운":                          "운송·창고",
    "항공":                          "운송·창고",
    "육운":                          "운송·창고",
    "물류창고":                      "운송·창고",
    # 기계·장비
    "기계":                          "기계·장비",
    "방위산업":                      "기계·장비",
    "산업기계":                      "기계·장비",
    # 철강·금속
    "철강":                          "금속",
    "비철금속":                      "금속",
    "금속제품":                      "금속",
    # 음식료·담배
    "식품":                          "음식료·담배",
    "음료":                          "음식료·담배",
    "담배":                          "음식료·담배",
    "식품유통":                      "음식료·담배",
    # 유통
    "유통":                          "유통",
    "소매":                          "유통",
    "인터넷유통":                    "유통",
    # IT서비스
    "소프트웨어":                    "IT 서비스",
    "인터넷서비스":                  "IT 서비스",
    "게임":                          "IT 서비스",
    "IT서비스":                      "IT 서비스",
    # 통신
    "통신서비스":                    "통신",
    "무선통신":                      "통신",
    # 전기·가스
    "전기":                          "전기·가스",
    "가스":                          "전기·가스",
    "에너지":                        "전기·가스",
    # 일반서비스
    "미디어":                        "일반서비스",
    "엔터테인먼트":                  "일반서비스",
    "광고":                          "일반서비스",
    "호텔레저":                      "일반서비스",
    "교육":                          "일반서비스",
    # 섬유·의류
    "섬유":                          "섬유·의류",
    "의류":                          "섬유·의류",
    "화장품":                        "섬유·의류",
    # 부동산
    "부동산":                        "부동산",
    "리츠":                          "부동산",
}

def krx_to_hts(krx_name):
    """KRX 표준산업명 → HTS 업종명 변환"""
    if not krx_name:
        return "기타"
    for key, hts in KRX_TO_HTS.items():
        if key in krx_name:
            return hts
    return krx_name  # 매핑 없으면 원본 반환


# ═══════════════════════════════════════
# 한투 API
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
        "fid_input_price_1":       "",
        "fid_input_price_2":       "",
        "fid_vol_cnt":             "",
    }
    try:
        res  = requests.get(url, headers=make_headers(token, "FHPST01740000"), params=params, timeout=10)
        data = res.json()
        if data.get("rt_cd") == "0" and data.get("output"):
            return [{"Code": r.get("mksc_shrn_iscd",""), "Name": r.get("hts_kor_isnm",""), "Rank": int(r.get("data_rank",0))} for r in data["output"]]
    except Exception as e:
        st.warning(f"종목 리스트 오류: {e}")
    return []

def fetch_sector_hts(token, code):
    """HTS 업종명 우선, 없으면 KRX→HTS 변환"""
    try:
        # 1순위: FHKST01010100 bstp_kor_isnm
        res  = requests.get(
            f"{BASE_URL}/uapi/domestic-stock/v1/quotations/inquire-price",
            headers=make_headers(token, "FHKST01010100"),
            params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": code},
            timeout=5
        )
        out = res.json().get("output", {})
        hts = out.get("bstp_kor_isnm", "")
        if hts:
            return hts

        # 2순위: CTPF1002R std_idst_clsf_cd_name → KRX→HTS 변환
        res2 = requests.get(
            f"{BASE_URL}/uapi/domestic-stock/v1/quotations/search-stock-info",
            headers=make_headers(token, "CTPF1002R"),
            params={"PRDT_TYPE_CD": "300", "PDNO": code},
            timeout=5
        )
        krx = res2.json().get("output", {}).get("std_idst_clsf_cd_name", "")
        return krx_to_hts(krx) if krx else "기타"
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
        if data.get("rt_cd") != "0":
            return []
        lstn_stcn = int(data["output1"].get("lstn_stcn", 0))
        if lstn_stcn == 0:
            return []
        rows = []
        for r in data.get("output2", []):
            try:
                price  = float(r.get("stck_clpr", 0))
                rows.append({"Date": r["stck_bsop_date"], "Price": price, "Marcap": int(price * lstn_stcn)})
            except Exception:
                continue
        return rows
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
        data = res.json()
        if data.get("rt_cd") == "0":
            return float(data["output1"].get("bstp_nmix_prpr", 0))
    except Exception:
        pass
    return 0.0


# ═══════════════════════════════════════
# 과거 데이터 일괄 수집 (100건씩 분할)
# ═══════════════════════════════════════

def fetch_history_bulk(token, top_stocks, start, end, label="과거 데이터 수집"):
    total   = len(top_stocks)
    bar     = st.progress(0, text=f"{label} 시작...")
    sec_map = {}

    # 업종 수집
    for i, s in enumerate(top_stocks):
        sec_map[s["Code"]] = fetch_sector_hts(token, s["Code"])
        bar.progress((i+1)/(total*2), text=f"업종 수집: {i+1}/{total}")
        time.sleep(0.04)

    # 과거 시세 수집 (한투 API 1회당 최대 100일)
    # 2년 = 730일 → 8번 분할 필요
    date_ranges = []
    s_dt = datetime.strptime(start, "%Y%m%d")
    e_dt = datetime.strptime(end,   "%Y%m%d")
    chunk_days = 90  # 안전하게 90일씩
    cur = s_dt
    while cur <= e_dt:
        chunk_end = min(cur + timedelta(days=chunk_days-1), e_dt)
        date_ranges.append((cur.strftime("%Y%m%d"), chunk_end.strftime("%Y%m%d")))
        cur = chunk_end + timedelta(days=1)

    records = []
    for i, s in enumerate(top_stocks):
        code, name, rank = s["Code"], s["Name"], s["Rank"]
        sector = sec_map.get(code, "기타")
        for dr_start, dr_end in date_ranges:
            rows = fetch_stock_history(token, code, dr_start, dr_end)
            for r in rows:
                records.append({
                    "Date":   pd.to_datetime(r["Date"]),
                    "Code":   code,
                    "Name":   name,
                    "Sector": sector,
                    "Marcap": r["Marcap"],
                    "Price":  r["Price"],
                    "Rank":   rank,
                })
            time.sleep(0.03)
        bar.progress((total + i+1)/(total*2), text=f"시세 수집: {i+1}/{total}")

    bar.empty()
    return pd.DataFrame(records) if records else pd.DataFrame(columns=COLS)


# ═══════════════════════════════════════
# Google Drive / 로컬 저장
# ═══════════════════════════════════════

def _try_sheets_client():
    try:
        import gspread
        from google.oauth2.service_account import Credentials

        # 1. st.secrets 시도
        raw = ""
        try:
            raw = st.secrets.get("GCP_SERVICE_ACCOUNT", "")
        except Exception:
            pass

        # 2. 직접 toml 파싱 (fallback)
        if not raw:
            try:
                import toml
                secrets_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    ".streamlit", "secrets.toml"
                )
                if os.path.exists(secrets_path):
                    secrets = toml.load(secrets_path)
                    raw = secrets.get("GCP_SERVICE_ACCOUNT", "")
            except Exception:
                pass

        if not raw:
            return None, None

        info  = json.loads(raw)
        creds = Credentials.from_service_account_info(info, scopes=[
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ])
        return gspread.authorize(creds), info
    except Exception as e:
        return None, None

def _try_open_spreadsheet(gc, info):
    """스프레드시트 ID로 직접 열기"""
    try:
        return gc.open_by_key(SPREADSHEET_ID)
    except Exception as e:
        st.sidebar.warning(f"Sheets 열기 오류: {e}")
        return None

def _parse_df(df):
    df["Date"]   = pd.to_datetime(df["Date"],   errors="coerce")
    df["Marcap"] = pd.to_numeric(df["Marcap"],  errors="coerce").fillna(0).astype(int)
    df["Price"]  = pd.to_numeric(df["Price"],   errors="coerce").fillna(0)
    df["Rank"]   = pd.to_numeric(df["Rank"],    errors="coerce").fillna(0).astype(int)
    return df.dropna(subset=["Date"])

def _trim(df):
    if df.empty:
        return df
    cutoff = pd.Timestamp.now() - pd.Timedelta(days=KEEP_DAYS)
    return df[df["Date"] >= cutoff].copy()

def load_data():
    # 1. Google Sheets 우선
    gc, info = _try_sheets_client()
    if gc and info:
        ss = _try_open_spreadsheet(gc, info)
        if ss:
            try:
                import gspread
                try:
                    ws = ss.worksheet("Data")
                except gspread.exceptions.WorksheetNotFound:
                    ws = ss.sheet1
                records = ws.get_all_records()
                if records:
                    df = _trim(_parse_df(pd.DataFrame(records)))
                    if not df.empty:
                        df.to_csv(LOCAL_CSV, index=False)
                        return df, "sheets"
                # 데이터 없어도 sheets 연동 상태 반환
                return pd.DataFrame(columns=COLS), "sheets_empty"
            except Exception as e:
                st.sidebar.warning(f"Sheets 로드 오류: {e}")

    # 2. 로컬 CSV 폴백
    if os.path.exists(LOCAL_CSV):
        try:
            df = _trim(_parse_df(pd.read_csv(LOCAL_CSV)))
            if not df.empty:
                return df, "local"
        except Exception:
            pass

    return pd.DataFrame(columns=COLS), "empty"

def save_data(new_df):
    df_copy = new_df.copy()
    df_copy["Date"] = df_copy["Date"].astype(str)

    # 로컬 CSV
    try:
        if os.path.exists(LOCAL_CSV):
            existing = pd.read_csv(LOCAL_CSV)
            combined = pd.concat([existing, df_copy[COLS]], ignore_index=True)
            combined.drop_duplicates(subset=["Date","Code"], keep="last", inplace=True)
            combined["Date"] = pd.to_datetime(combined["Date"], errors="coerce")
            combined = combined[combined["Date"] >= (pd.Timestamp.now() - pd.Timedelta(days=KEEP_DAYS))]
            combined["Date"] = combined["Date"].astype(str)
            combined.to_csv(LOCAL_CSV, index=False)
        else:
            df_copy[COLS].to_csv(LOCAL_CSV, index=False)
    except Exception as e:
        st.warning(f"CSV 저장 오류: {e}")

    # Google Sheets
    gc, info = _try_sheets_client()
    if gc and info:
        ss = _try_open_spreadsheet(gc, info)
        if ss:
            try:
                import gspread
                try:
                    ws = ss.worksheet("Data")
                except gspread.exceptions.WorksheetNotFound:
                    ws = ss.sheet1
                if not ws.get_all_values():
                    ws.append_row(COLS)
                # 중복 제거 후 신규 데이터만 추가
                existing_records = ws.get_all_records()
                if existing_records:
                    ex_df  = pd.DataFrame(existing_records)
                    ex_keys = set(zip(ex_df["Date"].astype(str), ex_df["Code"].astype(str)))
                    new_rows = [r for r in df_copy[COLS].values.tolist()
                                if (str(r[0]), str(r[1])) not in ex_keys]
                else:
                    new_rows = df_copy[COLS].values.tolist()
                if new_rows:
                    ws.append_rows(new_rows, value_input_option="RAW")
                return "sheets+local"
            except Exception as e:
                st.sidebar.warning(f"Sheets 저장 오류: {e}")
    return "local"


def load_events():
    """금융 일정 로드 (별도 스프레드시트)"""
    try:
        gc, info = _try_sheets_client()
        if not gc:
            return pd.DataFrame()
        ss  = gc.open_by_key(CALENDAR_ID)
        try:
            ws = ss.worksheet("Events")
        except Exception:
            ws = ss.sheet1
        records = ws.get_all_records()
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records)
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        return df.dropna(subset=["Date"])
    except Exception as e:
        return pd.DataFrame()

# ═══════════════════════════════════════
# 시각화
# ═══════════════════════════════════════

def get_scale_info(total_t):
    for i, cap in enumerate(SCALE_TIERS):
        if total_t <= cap:
            lo = SCALE_TIERS[i-1] if i > 0 else 0
            return f"{lo:,}조 ~ {cap:,}조", cap
    return f"{SCALE_TIERS[-1]:,}조 이상", SCALE_TIERS[-1] + 2000

def build_treemap(df, date_str, kospi_idx, total_min, total_max):
    """
    total_min, total_max: 2년간 시총 최솟값/최댓값 (조 단위)
    박스 크기: min이 max의 50% 이하로 내려가지 않도록 조정
    """
    if df.empty:
        fig = go.Figure()
        fig.add_annotation(text="데이터 없음", x=0.5, y=0.5, showarrow=False, font=dict(size=20))
        return fig

    total_t = df["Marcap"].sum() / 1e12
    tier_label, tier_cap = get_scale_info(total_t)

    # 박스 크기 비율 계산 (50% 하한선)
    if total_max > 0:
        raw_ratio = total_t / total_max          # 0~1
        min_ratio = 0.5                          # 최솟값 50%
        scale_ratio = max(raw_ratio, min_ratio)  # 50%~100%
    else:
        scale_ratio = 1.0

    # 트리맵 높이 조정 (비율 반영)
    base_height = 560
    chart_height = int(base_height * scale_ratio)
    chart_height = max(chart_height, 300)  # 최소 300px

    sec_sum = df.groupby("Sector")["Marcap"].sum()
    df = df.copy()
    df["Sector_Label"] = df["Sector"].apply(
        lambda s: f"{s}  ({sec_sum.get(s,0)/1e12:.0f}조)"
    )

    def get_color(name):
        if "삼성전자" in name and "우" not in name: return "#FFD700"
        if "SK하이닉스" in name:                    return "#FF8C00"
        return "#E5ECF6"

    df["_c"]  = df["Name"].apply(get_color)
    cmap      = {c: c for c in df["_c"].unique()}
    ref_pct   = min(100/total_t*100, 100) if total_t > 0 else 0

    import plotly.express as px
    fig = px.treemap(
        df,
        path=["Sector_Label", "Name"],
        values="Marcap",
        color="_c",
        color_discrete_map={c: c for c in df["_c"].unique()},
        custom_data=["Sector", "Price", "Rank", "Marcap"],
    )
    fig.update_traces(
        textinfo="label+percent parent",
        textfont=dict(size=11),
        hovertemplate=(
            "<b>%{label}</b><br>"
            "시가총액: %{customdata[3]:,.0f}원<br>"
            "주가: %{customdata[1]:,.0f}원<br>"
            "순위: %{customdata[2]}<br>"
            "섹터: %{customdata[0]}<extra></extra>"
        ),
    )

    fig.update_layout(
        title=dict(
            text=(f"📈 KOSPI {kospi_idx:,.2f}  |  {date_str}  |  "
                  f"총 {total_t:,.0f}조원  |  구간: {tier_label}  |  "
                  f"2년 최대 대비 {scale_ratio*100:.0f}%"),
            x=0.5, font=dict(size=13)
        ),
        margin=dict(t=55, l=5, r=120, b=5),
        height=chart_height,
        uniformtext=dict(minsize=9, mode="hide"),
        annotations=[dict(
            x=1.01, y=1, xref="paper", yref="paper",
            xanchor="left", yanchor="top",
            text=(f"<b>스케일 안내</b><br>"
                  f"■ 100조 ≈ 화면 {ref_pct:.1f}%<br>"
                  f"2년 최대: {total_max:,.0f}조<br>"
                  f"2년 최소: {total_min:,.0f}조<br>"
                  f"현재: {total_t:,.0f}조<br>"
                  f"크기 비율: {scale_ratio*100:.0f}%"),
            showarrow=False, font=dict(size=10, color="#444"),
            bgcolor="rgba(255,255,255,0.92)",
            bordercolor="#ccc", borderwidth=1, borderpad=6,
        )],
    )
    return fig

# 이벤트 타입별 색상
EVENT_COLORS = {
    "금리":     {"미국": "#E53935", "한국": "#FF7043"},
    "경제지표": {"미국": "#1565C0", "한국": "#42A5F5"},
    "실적":     {"미국": "#6A1B9A", "한국": "#AB47BC"},
    "기타":     {"미국": "#37474F", "한국": "#78909C"},
}

def get_event_color(country, etype):
    return EVENT_COLORS.get(etype, EVENT_COLORS["기타"]).get(country, "#888")

def build_trend_chart(df_all, selected_date, df_events=None):
    daily = (df_all.groupby("Date")["Marcap"].sum()
             .reset_index().rename(columns={"Marcap":"Total"}))
    daily["조"] = daily["Total"] / 1e12

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=daily["Date"], y=daily["조"],
        fill="tozeroy", mode="lines+markers",
        line=dict(color="#1565C0", width=2),
        marker=dict(size=4, color="#1565C0"),
        fillcolor="rgba(21,101,192,0.12)",
        name="시총 합계",
        hovertemplate="%{x|%Y-%m-%d}  %{y:,.0f}조원<extra></extra>",
    ))

    # 선택일 강조
    sel = daily[daily["Date"].dt.date == selected_date]
    if not sel.empty:
        fig.add_trace(go.Scatter(
            x=sel["Date"], y=sel["조"],
            mode="markers", showlegend=False,
            marker=dict(color="#E53935", size=12),
            hovertemplate="%{x|%Y-%m-%d}  %{y:,.0f}조원<extra></extra>",
        ))

    # 스케일 구간 점선
    ymin = daily["조"].min()*0.97
    ymax = daily["조"].max()*1.03
    for t in SCALE_TIERS:
        if ymin < t < ymax*1.1:
            fig.add_hline(y=t, line=dict(color="orange", width=1, dash="dot"),
                          annotation_text=f"{t:,}조", annotation_position="right",
                          annotation_font_size=9)

    # 금융 일정 이벤트 표시
    if df_events is not None and not df_events.empty:
        date_min = daily["Date"].min()
        date_max = daily["Date"].max()
        ev = df_events[
            (df_events["Date"] >= date_min) &
            (df_events["Date"] <= date_max)
        ]
        for _, row in ev.iterrows():
            country = str(row.get("Country", "기타"))
            etype   = str(row.get("Type", "기타"))
            title   = str(row.get("Title", ""))
            desc    = str(row.get("Description", ""))
            color   = get_event_color(country, etype)
            dash    = "solid" if country == "미국" else "dot"
            flag    = "🇺🇸" if country == "미국" else "🇰🇷"

            fig.add_vline(
                x=str(row["Date"].date()),
                line=dict(color=color, width=1.2, dash=dash),
                annotation_text=f"{flag}{title}",
                annotation_textangle=-90,
                annotation_font_size=9,
                annotation_font_color=color,
                annotation_position="top",
            )
            # 호버용 invisible scatter
            match = daily[daily["Date"].dt.date == row["Date"].date()]
            y_val = match["조"].values[0] if not match.empty else ymax * 0.9
            fig.add_trace(go.Scatter(
                x=[row["Date"]], y=[y_val],
                mode="markers",
                marker=dict(color=color, size=8, symbol="diamond"),
                showlegend=False,
                hovertemplate=(
                    f"<b>{flag} {title}</b><br>"
                    f"날짜: {row['Date'].strftime('%Y-%m-%d')}<br>"
                    f"국가: {country} | 유형: {etype}<br>"
                    f"{desc}<extra></extra>"
                ),
            ))

    fig.update_layout(
        height=220,
        margin=dict(t=25, b=35, l=65, r=90),
        xaxis=dict(showgrid=False, title="", tickformat="%y.%m"),
        yaxis=dict(showgrid=True, title="조원"),
        showlegend=False,
        hovermode="x unified",
        plot_bgcolor="#FAFAFA",
        paper_bgcolor="white",
    )
    return fig


# ═══════════════════════════════════════
# MAIN
# ═══════════════════════════════════════

def check_password():
    """비밀번호 확인 — 통과하면 True 반환"""
    if st.session_state.get("authenticated"):
        return True

    st.title("🔐 KOSPI 트리맵 로그인")
    st.markdown("---")
    pw = st.text_input("비밀번호를 입력하세요", type="password", placeholder="Password")
    if st.button("로그인", use_container_width=True):
        correct = st.secrets.get("APP_PASSWORD", "")
        if not correct:
            # secrets 없으면 toml 직접 파싱
            try:
                import toml
                secrets_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    ".streamlit", "secrets.toml"
                )
                if os.path.exists(secrets_path):
                    correct = toml.load(secrets_path).get("APP_PASSWORD", "")
            except Exception:
                pass
        if pw == correct and correct:
            st.session_state["authenticated"] = True
            st.rerun()
        else:
            st.error("❌ 비밀번호가 틀렸습니다")
    st.stop()

def main():
    # 비밀번호 확인
    check_password()

    st.markdown("""
    <style>
        .block-container { padding-top: 0.8rem; }
        div[data-testid="stSidebarContent"] { padding-top: 1rem; }
    </style>
    """, unsafe_allow_html=True)

    st.title("📈 KOSPI 시가총액 트리맵 대시보드")

    # 토큰
    if "token" not in st.session_state:
        with st.spinner("한투 API 토큰 발급 중..."):
            st.session_state["token"] = get_access_token()
    token = st.session_state.get("token")
    if not token:
        st.error("❌ 한투 API 토큰 발급 실패")
        return

    # 데이터 로드
    with st.spinner("📂 데이터 로드 중..."):
        df_history, source = load_data()

    today = datetime.now().date()

    # ── 필요한 날짜 계산 (Google Sheets 확인 후 빠진 날짜만 수집) ──
    if df_history.empty:
        # 데이터 없음 → 2년치 전체 수집
        fetch_start = (today - timedelta(days=730)).strftime("%Y%m%d")
        fetch_end   = today.strftime("%Y%m%d")
        fetch_label = "2년치 초기 데이터 수집 (10~20분 소요)"
        need_fetch  = True
    else:
        # 마지막 날짜 확인 → 그 다음날부터 오늘까지만 수집
        last_date  = df_history["Date"].max().date()
        fetch_start = (last_date + timedelta(days=1)).strftime("%Y%m%d")
        fetch_end   = today.strftime("%Y%m%d")
        # 주말/공휴일 제외: 실제 영업일이 있을 때만 수집
        need_fetch  = last_date < today and fetch_start <= fetch_end
        fetch_label = f"{last_date + timedelta(days=1)} ~ {today} 누락 데이터 수집"

    if need_fetch:
        st.info(f"📡 {fetch_label}...")
        top_stocks = fetch_top_stocks(token)
        if top_stocks:
            hist_df = fetch_history_bulk(token, top_stocks, fetch_start, fetch_end, fetch_label)
            if not hist_df.empty:
                saved = save_data(hist_df)
                label = "Google Sheets + 로컬 CSV" if saved=="sheets+local" else "로컬 CSV"
                n_days = hist_df["Date"].dt.date.nunique()
                st.success(f"✅ {n_days}일치 {len(hist_df)}건 → {label} 저장 완료!")
                df_history = (
                    pd.concat([df_history, hist_df], ignore_index=True)
                    if not df_history.empty else hist_df
                )
            else:
                st.info("📅 새로운 거래일 데이터가 없습니다 (주말/공휴일)")

    existing_dates = set() if df_history.empty else set(
        df_history["Date"].dt.strftime("%Y-%m-%d").tolist()
    )

    if df_history.empty:
        st.warning("⚠️ 데이터가 없습니다.")
        if st.button("🔄 다시 시도"):
            st.session_state.pop("token", None)
            st.rerun()
        return

    kospi_idx       = get_kospi_index(token)
    available_dates = sorted(df_history["Date"].dt.date.unique())

    # 2년간 일별 시총 최대/최솟값 계산
    daily_totals = df_history.groupby("Date")["Marcap"].sum() / 1e12
    total_max = daily_totals.max()
    total_min = daily_totals.min()

    # ── 사이드바 ──
    with st.sidebar:
        st.header("🔍 필터 설정")

        if source in ("sheets", "sheets_empty"):
            st.success("☁️ Google Sheets 연동 중")
        else:
            st.info("💾 로컬 CSV 모드")

        if not df_history.empty:
            d_min  = df_history["Date"].min().strftime("%Y.%m.%d")
            d_max  = df_history["Date"].max().strftime("%Y.%m.%d")
            n_days = df_history["Date"].dt.date.nunique()
            st.info(f"📅 {d_min} ~ {d_max}\n총 {n_days}일치 (최대 {KEEP_DAYS//365}년)")

        latest_df   = df_history[df_history["Date"].dt.date == available_dates[-1]]
        all_sectors = sorted(latest_df["Sector"].unique().tolist())
        all_stocks  = sorted(latest_df["Name"].unique().tolist())

        st.subheader("섹터")
        sector_mode = st.radio("표시 방식", ["전체","선택 섹터만 표시","특정 섹터 제외"], key="sm")
        sel_sectors = all_sectors
        if sector_mode == "선택 섹터만 표시":
            sel_sectors = st.multiselect("표시할 섹터", all_sectors, default=all_sectors)
        elif sector_mode == "특정 섹터 제외":
            excl = st.multiselect("제외할 섹터", all_sectors, default=[])
            sel_sectors = [s for s in all_sectors if s not in excl]

        st.subheader("종목")
        excl_stocks = st.multiselect("제외할 종목", all_stocks, default=[])

        st.subheader("기타")
        top_n = st.slider("표시 종목 수", 30, 200, 100, 10)

        st.markdown("---")
        if st.button("🔄 오늘 데이터 갱신"):
            fetch_top_stocks.clear()
            get_kospi_index.clear()
            st.session_state.pop("token", None)
            st.rerun()
        if st.button("🗑️ 전체 재수집 (2년)"):
            if os.path.exists(LOCAL_CSV):
                os.remove(LOCAL_CSV)
            fetch_top_stocks.clear()
            get_access_token.clear()
            st.session_state.pop("token", None)
            st.rerun()

    # ── 날짜 슬라이더 + 버튼 이동 ──
    if len(available_dates) == 1:
        selected_date = available_dates[0]
        st.info(f"📅 {selected_date}")
    else:
        if "date_idx" not in st.session_state:
            st.session_state["date_idx"] = len(available_dates) - 1

        col_prev, col_next = st.columns([1, 1])
        with col_prev:
            prev_clicked = st.button("◀ 이전날", key="btn_prev")
        with col_next:
            next_clicked = st.button("다음날 ▶", key="btn_next")

        if prev_clicked:
            st.session_state["date_idx"] = max(0, st.session_state["date_idx"] - 1)
            st.rerun()
        if next_clicked:
            st.session_state["date_idx"] = min(len(available_dates)-1, st.session_state["date_idx"] + 1)
            st.rerun()

        selected_date = st.select_slider(
            "📅 날짜 선택  ◀ 과거 ─────────────── 현재 ▶",
            options=available_dates,
            value=available_dates[st.session_state["date_idx"]],
            format_func=lambda d: d.strftime("%Y.%m.%d"),
            key="date_slider",
        )
        st.session_state["date_idx"] = list(available_dates).index(selected_date)

    # ── 필터 ──
    df_day = df_history[df_history["Date"].dt.date == selected_date].copy()
    df_day = df_day.sort_values("Marcap", ascending=False).head(top_n)
    df_day = df_day[df_day["Sector"].isin(sel_sectors)]
    df_day = df_day[~df_day["Name"].isin(excl_stocks)]

    total_t = df_day["Marcap"].sum() / 1e12
    tier_label, _ = get_scale_info(total_t)

    # ── 전일 대비 ──
    prev_dates = [d for d in available_dates if d < selected_date]
    if prev_dates:
        prev_date    = prev_dates[-1]
        df_prev      = df_history[df_history["Date"].dt.date == prev_date]
        prev_total_t = df_prev["Marcap"].sum() / 1e12
        delta_t      = total_t - prev_total_t
        delta_pct    = (delta_t / prev_total_t * 100) if prev_total_t > 0 else 0
        sign  = "▲" if delta_t >= 0 else "▼"
        emoji = "🔴" if delta_t >= 0 else "🔵"
        st.markdown(f"{emoji} 전일 대비: **{sign} {abs(delta_t):,.0f}조원 ({delta_pct:+.2f}%)**  |  전일({prev_date}): {prev_total_t:,.0f}조원")

    # ── KPI ──
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("KOSPI",       f"{kospi_idx:,.2f}")
    c2.metric("총 시가총액",  f"{total_t:,.0f}조원")
    c3.metric("표시 종목",    f"{len(df_day)}개")
    c4.metric("표시 섹터",    f"{df_day['Sector'].nunique()}개")
    c5.metric("스케일 구간",  tier_label)

    # ── 트리맵 (시총 비례 박스) ──
    st.plotly_chart(
        build_treemap(df_day, str(selected_date), kospi_idx, total_min, total_max),
        use_container_width=True
    )

    # ── 금융 일정 로드 ──
    df_events = load_events()

    # ── 시총 추이 차트 (하단 스크롤 역할) ──
    if len(available_dates) > 1:
        legend_parts = []
        if not df_events.empty:
            if "미국" in df_events["Country"].values: legend_parts.append("🇺🇸실선=미국")
            if "한국" in df_events["Country"].values: legend_parts.append("🇰🇷점선=한국")
            type_colors = {"금리":"🔴","경제지표":"🔵","실적":"🟣","기타":"⚫"}
            for t in df_events["Type"].unique():
                if t in type_colors: legend_parts.append(f"{type_colors[t]}{t}")
        legend_str = "  |  ".join(legend_parts)
        st.markdown(f"##### 📊 일별 시가총액 추이  {legend_str}")
        st.plotly_chart(build_trend_chart(df_history, selected_date, df_events), use_container_width=True)

    # ── 테이블 ──
    with st.expander("📋 종목 데이터 테이블"):
        show_df = df_day[["Rank","Name","Sector","Marcap","Price"]].copy()
        show_df["시총(조)"] = (show_df["Marcap"]/1e12).round(2)
        show_df = show_df.rename(columns={"Rank":"순위","Name":"종목명","Sector":"업종(HTS)","Price":"주가"}).drop(columns=["Marcap"])
        st.dataframe(show_df.reset_index(drop=True), use_container_width=True, height=300)


if __name__ == "__main__":
    main()
