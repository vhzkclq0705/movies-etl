import pandas as pd

BASE_DIR = "/Users/joon/swcamp4/data"

def fillna_meta(prev_df, cur_df):
    if prev_df is None or prev_df.empty:
        return cur_df
    # 데이터셋이 1만개 정도로 작을 때 성능이 좋음
    # prev_df = prev_df.set_index("movieCd")
    # cur_df = cur_df.set_index("movieCd")
    # df = prev_df.combine_first(cur_df).reset_index()
    # return df
    
    # 데이터셋이 10만단위가 넘을 때 성능이 좋음
    merged_df = prev_df.merge(cur_df, on="movieCd", how="outer", suffixes=("_A", "_B"))
    merged_df["multiMovieYn"] = merged_df["multiMovieYn_A"].combine_first(merged_df["multiMovieYn_B"])
    merged_df["repNationCd"] = merged_df["repNationCd_A"].combine_first(merged_df["repNationCd_B"])
    merged_df = merged_df.drop(columns=["multiMovieYn_A", "multiMovieYn_B", "repNationCd_A", "repNationCd_B"])
    return merged_df

def save_meta(dt: str, dag_id: str):
    prev_df = pd.read_parquet(f"{BASE_DIR}/{dag_id}", engine="pyarrow")
    if "0101" in dt:
        prev_df = None
        
    cur_df = pd.read_parquet(f"{BASE_DIR}/movies/merge/dailyboxoffice/dt=" + dt, engine="pyarrow")
    
    df = fillna_meta(prev_df, cur_df)
    df.to_parquet(f"{BASE_DIR}/{dag_id}/meta", engine="pyarrow", compression="snappy")
    return f"{BASE_DIR}/{dag_id}/meta"

def gen_movie(dt: str, dag_id: str):
    df = pd.read_parquet(f"{BASE_DIR}/{dag_id}/meta", engine='pyarrow')
    df.to_parquet(f"{BASE_DIR}/{dag_id}/dailyboxoffice", partition_cols=['dt', 'multiMovieYn', 'repNationCd'], engine="pyarrow", compression="snappy")
    return F"{BASE_DIR}/{dag_id}/dailyboxoffice"