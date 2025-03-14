
def gen_url(dt="20120101", url_param={}):
    import requests
    import os
    BASE_URL = "http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json?"
    API_KEY = os.getenv("MOVIES_API_KEY")
    
    url = f"{BASE_URL}?key={API_KEY}&targetDt={dt}"
    
    for k, v in url_param.items():
        url += f"&{k}={v}"
    
    return url
    

def get_movies_data(targetDt, url_params={}):
    """
    영화진흥위원회 API를 사용하여 특정 날짜의 박스오피스 데이터를 가져오는 함수.
    
    :param targetDt: 조회할 날짜 (YYYYMMDD 형식, 필수)
    :param itemPerPage: 한 페이지당 출력 건수 (선택)
    :param multiMovieYn: 다양성 영화 여부 (선택)
    :param repNationCd: 제작 국가 코드 (선택)
    :param wideAreaCd: 상영 지역 코드 (선택)
    :return: API 응답 데이터 (JSON 형식)
    """
    import requests
    import os
    
    BASE_PATH = "/Users/joon/"
    BASE_URL = "http://kobis.or.kr/kobisopenapi/webservice/rest/boxoffice/searchDailyBoxOfficeList.json"
    API_KEY = os.getenv("MOVIES_API_KEY")
    
    # API 파라미터 구성
    params = {
        "key": API_KEY,
        "targetDt": targetDt
    }
    params.update({k: v for k, v in url_params.items() if v})
    
    # API 요청
    response = requests.get(BASE_URL, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        return None