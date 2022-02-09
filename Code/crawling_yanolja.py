import json
import time
import parmap
import requests

from multiprocessing import Pool
from bs4 import BeautifulSoup
from tqdm import tqdm
from util import dbhelper


# 호텔 key값을 가져오기 (seedTable)
def crawl_info(page_num):
    url = "https://www.yanolja.com/api/v1/contents/search?advert=AREA&capacityAdult=2&capacityChild=0&hotel=1&lat=37.50681&lng=127.06624&page=" + str(page_num) + "&region=900582&rentType=1&limit=20&searchType=hotel&sort=106&stayType=1&themes=&coupon=&excludeSoldout=&includeRent=&includeStay="
    headers = {'User-Agent': 'Chrome/97.0.4692.99',
               'Referer': 'https://www.yanolja.com/hotel/r-900582?advert=AREA',
               'X-Requested-With': 'XMLHttpRequest',
               'Host': 'www.yanolja.com',
               'Accept': 'application/json, text/plain, */*',
               'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
               'Accept-Encoding': 'gzip, deflate, br',
               'Connection': 'keep-alive',
               'DNT': '1',
               'sec-ch-ua': '" Not;A Brand";v="99", "Google Chrome";v="97", "Chromium";v="97"',
               'sec-ch-ua-mobile': '?0',
               'sec-ch-ua-platform': 'macOS',
               'Sec-Fetch-Dest': 'empty',
               'Sec-Fetch-Mode': 'cors',
               'Sec-Fetch-Site': 'same-origin',
               'Cookie': 'cgntId=ap-northeast-2%3Af1d88933-8fd8-4dd0-a898-c2b4951331f3; _gcl_au=1.1.1930172394.1642645926; ACEUACS=1603435925250238033; ACEFCID=UID-61E8C9A614A5A2D2CA71883C; ACEUCI=1; _fbp=fb.1.1642645926265.1054114438; _ga=GA1.2.729287227.1642645926; SavedFiltersKey=/hotel/r-900582/hkey-hotel; __insp_wid=1723495738; __insp_slim=1642766038240; __insp_nv=true; __insp_targlpu=aHR0cHM6Ly9kb21lc3RpYy1vcmRlci1zaXRlLnlhbm9samEuY29tL3Jldmlldy9wcm9wZXJ0aWVzLzMwMTM0MTAvcmV2aWV3cw%3D%3D; __insp_targlpt=7JW864aA7J6QIOyYiOyVvQ%3D%3D; __insp_norec_sess=true; _gid=GA1.2.2095669861.1642766736; latestProduct={%22type%22:%22domesticPlacement%22%2C%22id%22:%2210042238%22}; yanolja_sid=s%3Ax0MYAXeBtO9iJiuvWaQWMV_V5qGyhueq.1xAYk54uPCc5sAAm4hq2L4WIY5haaM6saV%2F5KtB2BKE; location={%22latitude%22:%2237.6160234%22%2C%22longitude%22:%22126.9250306%22%2C%22address%22:%22%EC%84%9C%EC%9A%B8%ED%8A%B9%EB%B3%84%EC%8B%9C%20%EC%9D%80%ED%8F%89%EA%B5%AC%20%ED%86%B5%EC%9D%BC%EB%A1%9C%20796%22%2C%22addressShort%22:%22%EC%84%9C%EC%9A%B8%ED%8A%B9%EB%B3%84%EC%8B%9C%20%EC%9D%80%ED%8F%89%EA%B5%AC%20%ED%86%B5%EC%9D%BC%EB%A1%9C%22%2C%22addressOnlyRoad%22:%22%ED%86%B5%EC%9D%BC%EB%A1%9C%22}; wcs_bt=ae93a192ec48a4:1643018093; _AceT=fb.1.1642645926265.1054114438; _gat=1'
               }
    try:
        res = requests.get(url, headers=headers)
    except:
        time.sleep(2)
        res = requests.get(url, headers=headers)
    
    data = json.loads(res.text)
    info = []                                       # 호텔 세부정보 리스트
    hotels_info = {}                                # 호텔 세부정보 리스트 안에 들어갈 딕셔너리
    
    lists = data['motels']
    if data['motels']['counts'] == 0:               # 가져올 호텔 정보가 더 이상 없으면 return
        return
    
    # 호텔 정보가 담겨 있는 lists의 값을 가져오기
    for k, v in lists.items():
        if k == 'lists':
            hotel_list = v
    
    # 호텔 리스트에서 hotels_info 딕셔너리에 값 저장 후 info 리스트에 값 추가
    for i in hotel_list:
        addr2 = crawl_detail(i['key'])
        hotels_info['key'] = i['key']
        hotels_info['site'] = 0                     # 야놀자(0) 여기어때(1) site 구분
        hotels_info['name'] = i['name']
        hotels_info['addr1'] = i['addr1']           # 지번 주소
        hotels_info['addr2'] = addr2[0]['addr2']    # 도로명
        info.append(dict(hotels_info))

    return info


def crawl_detail(key):
    url = "https://www.yanolja.com/hotel/" + key
    try:
        res = requests.get(url)
    except:
        time.sleep(2)
        res = requests.get(url)

    soup = BeautifulSoup(res.text, 'html.parser')

    detail = []
    hotel_info = {}
    convenience = []
    try:
        conv_soup = soup.find('div', {'class': 'swiper-container Swiper_placeholder__27VGp'})
        conv_soup = conv_soup.find_all('p')
        for i in conv_soup:
            convenience.append(i.text)
    except:
        pass
    
    snail = soup.find_all('script', {'type': 'application/ld+json'})[2]
    data = json.loads(snail.text)

    hotel_info['conv'] = convenience
    hotel_info['addr2'] = data['address']['addressLocality']        # 도로명주소
    hotel_info['score'] = float(data['aggregateRating']['ratingValue'])
    hotel_info['review_count'] = int(data['aggregateRating']['reviewCount'])
    hotel_info['img_path'] = data['image'][0]
    hotel_info['grade'] = soup.find('div', {'class': 'PlaceDetailTitle_gradeTitle__Mrf1u'}).text
    if hotel_info['grade'] in ['1성급', '2성급', '3성급', '4성급', '5성급']:
        hotel_info['grade'] = int(hotel_info['grade'][:1])
    else:       # 등급이 없는 나머지 '프리미엄 호텔', '일반 호텔', '레지던스' 등이라면
        hotel_info['grade'] = 0
    
    detail.append(hotel_info)
    
    return detail


def crawl_review(key, page_num):
    url = "https://domestic-order-site.yanolja.com/dos-server/review/properties/" + key + "/reviews?size=20&sort=best:desc&page=" + str(page_num)
    try:
        res = requests.get(url)
    except:
        time.sleep(2)
        res = requests.get(url)

    data = json.loads(res.text)
    
    if data['meta']['exposedReviewPages'] < page_num:
        return
    
    review = []                         # 리뷰 정보 리스트
    review_info = {}                    # 리뷰 정보 리스트 안에 들어갈 딕셔너리
    reviews = data['reviews']
    review_info['total_cnt'] = data['meta']['exposedReviewCount']       # 총 리뷰 수
    review_info['total_page'] = data['meta']['exposedReviewPages']      # 총 리뷰 페이지
    
    for i in reviews:
        review_info['hotel_key'] = str(i['product']['propertyId'])
        review_info['review_key'] = str(i['id'])
        review_info['review'] = i['userContent']['content']
        review_info['score'] = i['userContent']['totalScore']
        review.append(dict(review_info))
        
    return review


all_info = []
all_key = []
# 야놀자에 등록되어 있는 서울의 모든 호텔 정보 가져오기
def crawl_hotels_info(s, e):
    for i in tqdm(range(s, e)):
        print(i)
        info = crawl_info(i)
        for k in info:
            all_key.append(k['key'])
            all_info.append(k)
            

# DB에서 호텔 키값 가져오기
def get_hotel_key():
    db = dbhelper()
    temp = db.select('select HOTEL_KEY from seed where HOTEL_SITE = 0')
    hotel_key = []
    for i in tqdm(temp):
        for key, value in i.items():
            hotel_key.append(value)
    
    return hotel_key


# DB에서 호텔 정보 가져오기
def get_hotel_info():
    db = dbhelper()
    temp = db.select('select * from seed where HOTEL_SITE = 0')
    hotel_info = []
    for i in tqdm(temp):
        dic_temp = {}
        for k, v in i.items():
            dic_temp['{}'.format(k)] = v
        hotel_info.append(dic_temp)
            
    return hotel_info


# seed_table에 호텔 기본 정보 insert
def seed_table():
    db = dbhelper()
    for k in tqdm(all_info):
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
    # k = get_hotel_key()
    page_num = 1
    review_count = 0
    while True:
        review = crawl_review(k, page_num)
        try:
            for r in review:
                innerDic = {}
                innerDic['HOTEL_KEY'] = r['hotel_key']
                innerDic['HOTEL_REVIEW_KEY'] = r['review_key']
                innerDic['HOTEL_REVIEW'] = r['review']
                innerDic['HOTEL_REVIEW_SCORE'] = r['score']
                review_count += 1

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
    info = get_hotel_info()
    for i in tqdm(info):
        detail = crawl_detail(i['HOTEL_KEY'])
        innerDic = {}
        innerDic['HOTEL_KEY'] = i['HOTEL_KEY']
        innerDic['HOTEL_SITE'] = i['HOTEL_SITE']
        innerDic['HOTEL_NAME'] = i['HOTEL_NAME']
        innerDic['HOTEL_ADDR_1'] = i['HOTEL_ADDR_1']
        innerDic['HOTEL_ADDR_2'] = i['HOTEL_ADDR_2']
        innerDic['HOTEL_GRADE'] = detail[0]['grade']
        innerDic['HOTEL_SCORE'] = detail[0]['score']
        innerDic['REVIEW_COUNT'] = detail[0]['review_count']
        innerDic['IMG_PATH'] = detail[0]['img_path']

        verify = {'와이파이': 'WIFI', '주차': 'PARK', '주차장': 'PARK', '주차가능': 'PARK', '발렛': 'PARK', '발렛파킹': 'PARK',
                  '피트니스': 'FITNESS', '사우나': 'SAUNA', '어메니티': 'AMENITY', '욕실용품': 'AMENITY', '레스토랑': 'FOOD', '식당': 'FOOD'}
        ans = {'WIFI': 0, 'PARK': 0, 'FITNESS': 0, 'SAUNA': 0, 'AMENITY': 0, 'FOOD': 0}

        for k, v in verify.items():
            if k in detail[0]['conv']:
                ans['%s' % v] = 1
            innerDic['%s' % v] = ans['%s' % v]

        db.insert('crwl', innerDic)


# crwl_review_table
# if __name__ == '__main__':
#     start_time = time.time()
    
#     pool = Pool(processes=12)
#     k = get_hotel_key()
#     # pool.map(crwl_review_table, k)
#     parmap.map(crwl_review_table, k, pm_pbar=True, pm_processes=12)
#     pool.close()
#     pool.join()
    
#     print('({:.2f}sec'.format(time.time() - start_time))


