import pandas as pd

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

def save_meta(dt: str, base_path: str):
    prev_df = None if "0101" in dt else pd.read_parquet(f"{base_path}/meta", engine="pyarrow")
    cur_df = pd.read_parquet(f"/Users/joon/swcamp4/data/movies/merge/dailyboxoffice/dt={dt}", engine="pyarrow")
    
    df = fillna_meta(prev_df, cur_df)
    df["dt"] = dt
    df.to_parquet(f"{base_path}/meta/meta", engine="pyarrow", compression="snappy")
    return f"{base_path}/meta"

def gen_movie(dt: str, base_path: str):
    df = pd.read_parquet(f"{base_path}/meta", engine='pyarrow')
    df.to_parquet(f"{base_path}/dailyboxoffice", partition_cols=['dt', 'multiMovieYn', 'repNationCd'], engine="pyarrow", compression="snappy")
    return F"{base_path}/dailyboxoffice"