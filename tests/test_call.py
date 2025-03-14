import os
from movies_etl.api.call import gen_url

def test_gen_url_default():
    r = gen_url()
    print(r)
    assert "kobis" in r
    assert "targetDt" in r
    assert os.getenv("MOVIES_API_KEY") in r

def test_gen_url_default_with_params():
    r = gen_url(url_param={"multiMovieYn": "Y", "repNationCd": "K"})
    assert "&multiMovieYn=Y" in r
    assert "&repNationCd=K" in r