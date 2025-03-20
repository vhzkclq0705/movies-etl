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
    df = pd.DataFrame(data)
    df["dt"] = dt
    df["audiCnt"] = df["audiCnt"].apply(pd.to_numeric)
    for k, v in url_params.items():
        df[k] = v
    
    return df

def save_df(df: pd.DataFrame, base_path: str, partitions=['dt']):
    df.to_parquet(base_path, partition_cols=partitions, engine="pyarrow", compression="snappy")
    save_path = base_path
    for p in partitions:
        save_path += f"/{p}={df.at[0, p]}"
    return save_path

def merge_df(dt: str, base_path: str):
    path = f"{base_path}/dt={dt}"
    not_use_cols = ["rnum", "rank", "rankInten", "rankOldAndNew", "openDt", "salesShare"]
    param_cols = ["multiMovieYn", "repNationCd"]
    
    df = pd.read_parquet(path, engine="pyarrow")
    df.drop(columns=not_use_cols)
    
    def resolve_value(series):
        value = series.dropna().unique()
        return value[0] if value else None
    
    agg_dict = {col: "first" for col in list(set(df.columns) - set(param_cols))}
    agg_dict.update({col: resolve_value for col in param_cols})
 
    gdf = df.groupby("movieCd", dropna=False).agg(agg_dict).reset_index()
    sdf = gdf.sort_values(by='audiCnt', ascending=False).reset_index(drop=True)
    sdf["rnum"] = sdf.index + 1
    sdf["rank"] = sdf["rnum"]
    
    sdf.to_parquet(f"/Users/joon/swcamp4/data/movies/merge/dailyboxoffice/dt={dt}", engine="pyarrow", compression="snappy")
    
    return sdf