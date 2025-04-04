import pandas as pd

def fillna_meta(prev_df, cur_df):
    if prev_df is None or prev_df.empty:
        return cur_df

    cols = [
        "movieNm", "rank", "rnum", 
        "salesAmt", "salesInten", "salesChange", "salesAcc",
        "audiCnt", "audiInten", "audiChange", "audiAcc",
        "scrnCnt", "showCnt",
        "multiMovieYn", "repNationCd", "dt"
    ]
    drop_cols = [f"{c}_A" for c in cols] + [f"{c}_B" for c in cols]
    
    merged_df = prev_df.merge(cur_df, on="movieCd", how="outer", suffixes=("_A", "_B"))
    for c in cols:
        merged_df[c] = merged_df[f"{c}_A"].combine_first(merged_df[f"{c}_B"])
    merged_df.drop(columns=drop_cols, inplace=True)

    return merged_df

def save_meta(dt: str, base_path: str):
    prev_df = None if "0101" in dt else pd.read_parquet(f"{base_path}/meta", engine="pyarrow")
    cur_df = pd.read_parquet(f"/Users/joon/swcamp4/data/movies/merge/dailyboxoffice/dt={dt}", engine="pyarrow")
    
    df = fillna_meta(prev_df, cur_df)
    df.to_parquet(f"{base_path}/meta/meta", engine="pyarrow", compression="snappy")
    return f"{base_path}/meta"

def gen_movie(dt: str, base_path: str):
    df = pd.read_parquet(f"{base_path}/meta", engine='pyarrow')
    df.to_parquet(f"{base_path}/dailyboxoffice", partition_cols=['dt', 'multiMovieYn', 'repNationCd'], engine="pyarrow", compression="snappy")
    return F"{base_path}/dailyboxoffice"