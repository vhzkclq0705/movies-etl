import os
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
API_KEY = os.getenv("MOVIES_API_KEY")

def gen_url(dt: str, url_param={}):
    url = f"{BASE_URL}?key={API_KEY}&targetDt={dt}"
    for k, v in url_param.items():
        url += f"&{k}={v}"
    print(url)
    
    return url

def call_api(dt: str, url_param={}):
    url = gen_url(dt, url_param)
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        result = data['boxOfficeResult']['dailyBoxOfficeList']
        return result
    else:
        return None

def list2df(data: list, dt: str, url_params={}):
    num_cols = [
        "rnum", "rank", "rankInten",
        "salesAmt", "salesShare", "salesInten", "salesChange", "salesAcc",
        "audiCnt", "audiInten", "audiChange",
        "scrnCnt", "showCnt"
    ]
    
    df = pd.DataFrame(data)
    df["dt"] = dt
    df[num_cols] = df[num_cols].apply(pd.to_numeric)
    for k, v in url_params.items():
        df[k] = v
    
    return df

def save_df(df: pd.DataFrame, base_path: str, partitions=['dt']):
    df.to_parquet(base_path, partition_cols=partitions)
    save_path = base_path
    for p in partitions:
        save_path += f"/{p}={df.at[0, p]}"
    return save_path