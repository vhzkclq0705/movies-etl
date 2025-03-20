import pandas as pd

BASE_URL = "/Users/joon/swcamp4/data/movies/merge/dailyboxoffice/dt="

def fillna_meta(prev_df: pd.DataFrame, cur_df: pd.DataFrame):
    if not prev_df:
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