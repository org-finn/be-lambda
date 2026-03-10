import os
import json
import time
import requests
from dotenv import load_dotenv

# 1. .env 파일로부터 환경 변수 로드
load_dotenv()

APP_KEY = os.getenv("KIS_APP_KEY")
APP_SECRET = os.getenv("KIS_APP_SECRET")
BASE_URL = "https://openapi.koreainvestment.com:9443"

# 2. 검증할 종목 리스트 (거래소코드, 종목코드)
# 거래소코드 예시: NAS(나스닥), NYS(뉴욕), AMS(아멕스)
stocks_to_verify = [
    {"excd": "NAS", "symb": "RGTI"},
    {"excd": "NYS", "symb": "OKLO"},
    {"excd": "NAS", "symb": "MSFT"},
    {"excd": "NAS", "symb": "AMZN"},
    {"excd": "NAS", "symb": "TSLA"},
    {"excd": "NAS", "symb": "NVDA"},
    {"excd": "NAS", "symb": "NFLX"},
    {"excd": "NAS", "symb": "ADBE"},
    {"excd": "NAS", "symb": "QCOM"},
    {"excd": "NAS", "symb": "INTC"},
    {"excd": "NAS", "symb": "AVGO"},
    {"excd": "NAS", "symb": "COST"},
    {"excd": "NAS", "symb": "GOOGL"},
    {"excd": "NYS", "symb": "WMT"},
    {"excd": "NYS", "symb": "JPM"},
    {"excd": "NYS", "symb": "JNJ"},
    {"excd": "NAS", "symb": "AMD"},
    {"excd": "NYS", "symb": "XOM"},
    {"excd": "NYS", "symb": "KO"},
    {"excd": "NYS", "symb": "CRM"},
    {"excd": "NYS", "symb": "ORCL"},
    {"excd": "NYS", "symb": "BAC"},
    {"excd": "NYS", "symb": "GS"},
    {"excd": "NYS", "symb": "PG"},
    {"excd": "NYS", "symb": "HD"},
    {"excd": "NYS", "symb": "MCD"},
    {"excd": "NYS", "symb": "LLY"},
    {"excd": "NAS", "symb": "META"},
    {"excd": "NYS", "symb": "UNH"},
    {"excd": "NYS", "symb": "PFE"},
    {"excd": "NYS", "symb": "DIS"},
    {"excd": "NYS", "symb": "V"},
    {"excd": "NAS", "symb": "AAPL"}
]

def get_access_token():
    """접근 토큰을 발급받습니다."""
    url = f"{BASE_URL}/oauth2/tokenP"
    headers = {"content-type": "application/json"}
    body = {
        "grant_type": "client_credentials",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET
    }
    res = requests.post(url, headers=headers, data=json.dumps(body))
    return res.json().get('access_token')

def verify_stock_code(token, excd, symb):
    """특정 종목의 호가를 조회하여 유효성을 검증합니다."""
    url = f"{BASE_URL}/uapi/overseas-price/v1/quotations/inquire-asking-price"
    
    # 헤더 설정
    headers = {
        "Content-Type": "application/json",
        "authorization": f"Bearer {token}",
        "appkey": APP_KEY,
        "appsecret": APP_SECRET,
        "tr_id": "HHDFS76200100", # 해외주식 호가 조회 TR ID
        "custtype": "P"           # 개인 고객
    }
    
    # 파라미터 설정
    params = {
        "AUTH": "",               # 공란 유지
        "EXCD": excd,             # 거래소 코드
        "SYMB": symb              # 종목 코드
    }

    res = requests.get(url, headers=headers, params=params)
    data = res.json()

    if res.status_code == 200 and data.get("rt_cd") == "0":
        print(f"[✅ 성공] {excd}:{symb} - 현재가: {data['output1']['last']}")
        return True
    else:
        error_msg = data.get('msg1', '알 수 없는 오류')
        print(f"[❌ 실패] {excd}:{symb} - 사유: {error_msg}")
        return False

# --- 실행부 ---
if __name__ == "__main__":
    print("인증 토큰을 발급받는 중...")
    access_token = get_access_token()

    if access_token:
        print(f"검증 시작 (총 {len(stocks_to_verify)}건)...\n")
        for stock in stocks_to_verify:
            verify_stock_code(access_token, stock['excd'], stock['symb'])
    else:
        print("토큰 발급에 실패했습니다. API Key와 Secret을 확인해주세요.")
