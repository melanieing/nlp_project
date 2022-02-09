import json
import time
import parmap
import requests

from multiprocessing import Pool
from bs4 import BeautifulSoup
from tqdm import tqdm
from util import dbhelper

# 여기어때 url page_num list
page_num_list = ['2012', '2019', '2016', '2014', '2015', '2020', '2018', '2017', '2021']

# 호텔 key값을 가져오기 (seedTable)
def crawl_info(page_num):
    url = "https://www.goodchoice.kr/product/search/2/" + page_num
    try:
        res = requests.get(url)
    except:
        time.sleep(2)
        res = requests.get(url)

    soup = BeautifulSoup(res.text, 'html.parser')

    info = []
    hotels_info = {}
    hotel_list = soup.find_all('li', {'class': 'list_2 adcno2'})
    for hotel in hotel_list:
        hotels_info['key'] = hotel.find('a').get('data-ano')
        hotels_info['site'] = 1                                         # 야놀자(0) 여기어때(1) site 구분
        hotels_info['name'] = hotel.find('strong').get_text()
        detail_info = crawl_detail(hotels_info['key'])                  # 주소 가져오기 위해 crawl_detail 함수 호출
        hotels_info['addr1'] = dict(detail_info[0])['addr1']            # 지번 주소
        hotels_info['addr2'] = dict(detail_info[0])['addr2']            # 도로명
        info.append(dict(hotels_info))

    return info


# 호텔 세부내용 url 받아서 주소, 등급, 평점 반환하기
def crawl_detail(key):
    url = "https://www.goodchoice.kr/product/detail?ano=" + key + "&adcno=2"
    try:
        res = requests.get(url)
    except:
        time.sleep(2)
        res = requests.get(url)

    # 호텔 세부 페이지의 html 저장
    soup = BeautifulSoup(res.text, 'html.parser')

    # 호텔 세부 페이지의 json 형식의 url 불러오기
    url = "https://www.goodchoice.kr/product/get_theme_list_non/detail?&ano=" + key
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        res = requests.get(url, headers=headers)
    except:
        time.sleep(2)
        res = requests.get(url, headers=headers)

    data = json.loads(res.text)
    conv_list = data['data']
    
    detail = []
    hotel_info = {}
    convenience = []
    for i in range(len(conv_list)):
        convenience.append(conv_list[i]['tminame'])

    img = soup.find_all('script', {'type': 'application/ld+json'})[0]
    data = json.loads(img.text)

    hotel_info['conv'] = convenience
    hotel_info['name'] = soup.find('div', {'class': 'info'}).find('h2').text
    hotel_info['addr1'] = soup.find('p', {'class': 'address'}).text
    hotel_info['addr2'] = data['address']['addressLocality']
    hotel_info['grade'] = soup.find('span', {'class': 'build_badge'}).text
    if hotel_info['grade'] in ['1성급', '2성급', '3성급', '4성급', '5성급']:
        hotel_info['grade'] = int(hotel_info['grade'][:1])
    else:       # 등급이 없는 나머지 '프리미엄 호텔', '일반 호텔', '레지던스' 등이라면
        hotel_info['grade'] = 0
    try:
        score1 = soup.find('div', {'class': 'score_cnt'})
        score2 = score1.find('span').text
        hotel_info['score'] = score2
    except:
        hotel_info['score'] = float(0)

    img1 = soup.find('div', {'class':'gallery_m index_mobile'})
    img2 = img1.find('img').get('data-src')
    hotel_info['img_path'] = img2

    detail.append(dict(hotel_info))

    return detail



def crawl_review(key, page_num):
    url = "https://www.goodchoice.kr/product/get_review_non/detail?page="+ str(page_num) + "&ano=" + key
    headers = {'User-Agent': 'Mozilla/5.0'}
    try:
        res = requests.get(url, headers=headers)
    except:
        time.sleep(2)
        res = requests.get(url, headers=headers)

    data = json.loads(res.text)
    result = data['result']

    review = []
    review_info = {}

    if result['total_page_cnt'] >= page_num:
        for k, v in result.items():
            if k == 'count':
                count = v                   # 등록된 review의 개수
            if k == 'items':
                items = v                   # 리뷰들이 담겨있는 데이터 리스트
                if not items:
                    review_info['total_page'] = result['total_page_cnt']
                    review.append(dict(review_info))
                    return review

        for i in items:
            review_info['hotel_key'] = key
            review_info['review_key'] = i['aepreg']
            review_info['review'] = i['aepcont']
            review_info['score'] = float(i['epilrate'])
            review_info['total_page'] = result['total_page_cnt']
            review.append(dict(review_info))

    else:
        return

    return review


# DB seed table에서 hotel_key 가져오기
def get_hotel_key():
    db = dbhelper()
    temp = db.select('select HOTEL_KEY from seed where HOTEL_SITE = 1')
    hotel_key = []
    for i in tqdm(temp):
        for key, value in i.items():
            hotel_key.append(value)

    return hotel_key


# DB에서 호텔 정보 가져오기
def get_hotel_info():
    db = dbhelper()
    temp = db.select('select * from seed where HOTEL_SITE = 1')
    hotel_info = []
    for i in tqdm(temp):
        dic_temp = {}
        for k, v in i.items():
            dic_temp['{}'.format(k)] = v
        hotel_info.append(dic_temp)
            
    return hotel_info


# seed table에 호텔 기본 정보 insert
def seed_table():
    db = dbhelper()
    for i in tqdm(page_num_list):
        info = crawl_info(i)

        for k in tqdm(info):
            innerDic = {}
            innerDic['HOTEL_KEY'] = k['key']
            innerDic['HOTEL_NAME'] = k['name']
            innerDic['HOTEL_SITE'] = k['site']
            innerDic['HOTEL_ADDR_1'] = k['addr1']
            innerDic['HOTEL_ADDR_2'] = k['addr2']

            db.insert('seed', innerDic)


# crwl_review_table에 리뷰 정보 insert
def crwl_review_table(k):
    db = dbhelper()
    # key_list = get_hotel_key()
    page_num = 1
    while True:
        try:
            review = crawl_review(k, page_num)
            for r in review:
                innerDic = {}
                innerDic['HOTEL_KEY'] = r['hotel_key']
                innerDic['HOTEL_REVIEW_KEY'] = r['review_key']
                innerDic['HOTEL_REVIEW'] = r['review']
                innerDic['HOTEL_REVIEW_SCORE'] = r['score']

                db.insert('crwl_review', innerDic)
            
        except:
            pass
        page_num += 1
        if review[0]['total_page'] < page_num:
            print('호텔키 = {} insert 끝'.format(k))
            break


# crwl_table에 호텔 상세 정보 insert
def crwl_table():
    db = dbhelper()
    key_list = get_hotel_key()
    for key in tqdm(key_list):
        detail = crawl_detail(key)
        review = crawl_review(key, 1)

        innerDic = {}
        innerDic['HOTEL_KEY'] = key
        innerDic['HOTEL_SITE'] = 1
        innerDic['HOTEL_NAME'] = detail[0]['name']
        innerDic['HOTEL_ADDR_1'] = detail[0]['addr1']
        innerDic['HOTEL_ADDR_2'] = detail[0]['addr2']
        innerDic['HOTEL_GRADE'] = detail[0]['grade']

        try:
            innerDic['HOTEL_SCORE'] = detail[0]['score']
        except:
            innerDic['HOTEL_SCORE'] = float(0)
        innerDic['IMG_PATH'] = detail[0]['img_path']
        try:
            innerDic['REVIEW_COUNT'] = review[0]['review_count']
        except:
            innerDic['REVIEW_COUNT'] = int(0)

        verify = {'와이파이': 'WIFI', '주차': 'PARK', '주차장': 'PARK', '주차가능': 'PARK', '발렛': 'PARK', '발렛파킹': 'PARK',
                  '피트니스': 'FITNESS', '사우나': 'SAUNA', '어메니티': 'AMENITY', '욕실용품': 'AMENITY', '레스토랑': 'FOOD', '식당': 'FOOD'}
        ans = {'WIFI': 0, 'PARK': 0, 'FITNESS': 0, 'SAUNA': 0, 'AMENITY': 0, 'FOOD': 0}

        for k, v in verify.items():
            if k in detail[0]['conv']:
                ans['%s' % v] = 1
            innerDic['%s' % v] = ans['%s' % v]

        db.insert('crwl', innerDic)


# if __name__ == '__main__':
#     start_time = time.time()
#     pool = Pool(processes=8)
#     k = get_hotel_key()
#     # pool.map(crwl_review_table, k)
#     parmap.map(crwl_review_table, k, pm_pbar=True, pm_processes=8)
#     pool.close()
#     pool.join()
#
#     print('({:.2f}sec'.format(time.time() - start_time))




