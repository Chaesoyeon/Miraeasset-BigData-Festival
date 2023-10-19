import boto3
import pandas as pd
import numpy as np
import pickle
import os
import re
from transformers import pipeline
import requests
from bs4 import BeautifulSoup
import os
import datetime as dt
import pandas as pd
from yahooquery import Ticker
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
from pykrx import stock
import io
import boto3
from s3_setting import *

#국내 종목코드에서 'A' 제거하고 숫자만 남기기
def kr_community_get_ticker_list(data):
    data['종목코드'] = data['종목코드'].str[1:]
    new_data = data.copy()
    return data, new_data


# 네이버 주식 토론방 크롤링
def kr_community_crawler(code, korea):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3)'}
    total_data = []
    
    # 날짜 설정(전날 데이터 가져오기)
    end_date = datetime.today().date() - timedelta(days = 1)# 현재 날짜
    start_date = end_date - timedelta(days=1)  # 1일 전 날짜 계산

    page_num = 1
    while page_num <= 10:
        if page_num % 10 == 0:
            print(f'================== Page {page_num} is done ==================')
            
        url = f"https://finance.naver.com/item/board.nhn?code={str(code)}&page={str(page_num)}"
        result = requests.get(url, headers=headers)
        bs_obj = BeautifulSoup(result.content, "html.parser")
        table = bs_obj.find('table', {'class': 'type2'})
        if table :
            tt = table.select('tbody > tr')
        else :
            tt = []
            
        should_stop = False  # 데이터 수집 중단 여부

        for i in range(2, len(tt)):
            if len(tt[i].select('td > span')) > 0:
                date_str = tt[i].select('td > span')[0].text.split()[0]  # 시간 정보 제거
                date = datetime.strptime(date_str, "%Y.%m.%d").date()
                
                if date < start_date:
                    should_stop = True  # 데이터 수집 중단
                    break
                
                if date <= end_date:  # 하루 이내의 데이터만 수집
                    title = tt[i].select('td.title > a')[0]['title']
                    link = "https://finance.naver.com" + tt[i].select('td.title > a')[0]['href']
                    views = tt[i].select('td > span')[1].text
                    pos = tt[i].select('td > strong')[0].text
                    neg = tt[i].select('td > strong')[1].text
                    commuData = {'날짜': date_str, '제목': title, '조회': views, '공감': pos, '비공감': neg, '내용_링크': link}
                    total_data.append(commuData)
                    
                    res = requests.get(link, headers=headers)
                    soup = BeautifulSoup(res.text, 'lxml')
                    commu_content = soup.select_one('#body').text.replace('\n', '').replace('\t', '').replace('\r', '')
                    commuData['내용'] = commu_content
                
        if should_stop:
            break
            
        page_num += 1

    total_df = pd.DataFrame(total_data)
    total_df['종목코드'] = code
    total_df['기업'] = korea.loc[korea['종목코드'] == code, '기업'].unique()[0]
    
    return total_df


#클라우드에 업로드
def kr_community_upload(data, comp, s3_client, bucket_name):

    # 데이터프레임을 파일로 저장
    xlsx_filename = f'{comp}_주식토론방.xlsx'
    xlsx_buffer = io.BytesIO()
    data.to_excel(xlsx_buffer, index = False)
    xlsx_buffer.seek(0)
        
    # xlsx 파일 클라우드 업로드
    object_name = str(datetime.now().strftime('%Y%m%d')) + '/kr_community_crawling/' + str(xlsx_filename)
    s3_client.upload_fileobj(xlsx_buffer, bucket_name, object_name)
    print(f"Uploaded {xlsx_filename} to S3")

    
#main 실행 함수
def kr_community_crawling_func():
    # AWS S3 설정
    s3, bucket_name, _ = s3_setting()
    
    # 티커 리스트 생성
    df, _ = get_comp_list()
    korea, _ = kr_community_get_ticker_list(df)
    kr_codes = korea['종목코드'].unique()

    # 클라우드 폴더 생성
    object_name = str(datetime.now().strftime('%Y%m%d')) + '/kr_community_crawling/'
    s3.put_object(Bucket=bucket_name, Key=object_name)

    for code in kr_codes:
        result = kr_community_crawler(code, korea)
        comp = korea.loc[korea['종목코드'] == code, '기업'].unique()[0]
  
        kr_community_upload(result, comp, s3, bucket_name)

