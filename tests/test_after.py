import pandas as pd
from movies_etl.api.after import fillna_meta, save_meta

def test_fillna_meta():
    prev_df = pd.DataFrame(
        {
            "movieCd": ["1001", "1002", "1003"],
            "multiMovieYn": ["Y", "Y", "N"],
            "repNationCd": ["K", "F", None],
        }
    )
    cur_df = pd.DataFrame(
        {
            "movieCd": ["1001", "1003", "1004"],
            "multiMovieYn": [None, "Y", "Y"],
            "repNationCd": [None, "F", "K"],
        }
    )
    
    rdf = fillna_meta(prev_df, cur_df)
    assert not rdf.isnull().values.any(), "결과 데이터프레임에 NaN 값이 있습니다!"

def test_fillna_meta_none_prev_df():
    prev_df = None
    cur_df = pd.DataFrame(
        {
            "movieCd": ["1001", "1003", "1004"],
            "multiMovieYn": [None, "Y", "Y"],
            "repNationCd": [None, "F", "K"],
        }
    )
    
    rdf = fillna_meta(prev_df, cur_df)
    assert rdf.equals(cur_df), "rdf는 current_df와 동일해야 합니다!"
    
# def test_save_meta():
#     ymd = "20240101"
#     dag_id = "movie"
    
#     save_path = save_meta(dt=ymd, dag_id=dag_id)
    
#     assert save_path == f"/Users/joon/swcamp4/data/{dag_id}/meta"