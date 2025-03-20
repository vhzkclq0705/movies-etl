import os
import pandas as pd
from pandas.api.types import is_numeric_dtype
from movies_etl.api.call import gen_url, call_api, list2df, save_df, merge_df

def test_gen_url_default():
    r = gen_url("20120101")
    print(r)
    assert "kobis" in r
    assert "targetDt" in r
    assert os.getenv("MOVIES_API_KEY") in r

def test_gen_url_default_with_params():
    r = gen_url("20120101", url_param={"multiMovieYn": "Y", "repNationCd": "K"})
    assert "&multiMovieYn=Y" in r
    assert "&repNationCd=K" in r

def test_call_api():
    r = call_api("20120101")
    assert isinstance(r, list)
    assert isinstance(r[0]['rnum'], str)
    assert len(r) == 10
    for e in r:
        assert isinstance(e, dict)

def test_list2df():
    ymd = "20210101"
    data = call_api(dt=ymd)
    df = list2df(data, dt=ymd)
    print(df)
    assert isinstance(df, pd.DataFrame)
    assert len(data) == len(df)
    assert set(data[0].keys()).issubset(set(df.columns))
    assert "dt" in df.columns, "dt 컬럼이 있어야 함"
    assert (df["dt"] == ymd).all(), "모든 컬럼에 입력된 날짜 값이 존재해야 함"
    
def test_save_df():
    ymd = "20210101"
    data = call_api(dt=ymd)
    df = list2df(data, ymd)
    base_path = "~/swcamp4/temp/movie"
    r = save_df(df, base_path)
    assert r == f"{base_path}/dt={ymd}"
    read_df = pd.read_parquet(r)
    assert 'dt' not in read_df.columns
    assert 'dt' in pd.read_parquet(base_path).columns

def test_save_df_url_param():
    ymd = "20210101"
    url_param = {'multiMovieYn': "Y"}
    
    data = call_api(dt=ymd, url_param=url_param)
    df = list2df(data, ymd, url_param)
    base_path = "~/swcamp4/temp/movie"
    r = save_df(df, base_path, ['dt'] + list(url_param.keys()))
    assert r == f"{base_path}/dt={ymd}/multiMovieYn=Y"
    read_df = pd.read_parquet(r)
    assert 'dt' not in read_df.columns
    assert 'dt' in pd.read_parquet(base_path).columns

def test_list2df_check_num():
    ymd = "20210101"
    data = call_api(dt=ymd)
    df = list2df(data, ymd)
    
    assert is_numeric_dtype(df["audiCnt"]), f"audiCnt가 숫자가 아닙니다."

def test_merge_df():
    PATH = "~/swcamp4/data/movies/dailyboxoffice"
    ymd = "20240101"
    
    df = pd.read_parquet(f"{PATH}/dt={ymd}")
    
    new_df = merge_df(ymd, PATH)
    assert len(new_df) <= len(df)