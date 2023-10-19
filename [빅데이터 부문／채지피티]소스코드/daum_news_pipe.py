import boto3
import pandas as pd
import numpy as np
import pickle
import os
from datetime import timedelta
import re
from transformers import pipeline
import requests
from bs4 import BeautifulSoup
import os
import datetime
import pandas as pd
from io import BytesIO
from yahooquery import Ticker
import yfinance as yf
import pandas as pd
import datetime
from pykrx import stock
import io
from collections import Counter
from konlpy.tag import Komoran
from s3_setting import *


#기업리스트 불러오기(하나의 데이터셋으로 취합)
def get_daum_news():
    s3, bucket_name, _ = s3_setting()
    folder_prefix = str(datetime.datetime.now().strftime('%Y%m%d')) + '/daum_news_crawling/'

    result_list = []  # 모든 객체를 저장할 리스트
    paginator = s3.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)

    combined_dataframe = []
    dataframes = []
    for page in page_iterator:
        if 'Contents' in page:
            contents_list = page['Contents']
            for content in contents_list:
                key = content['Key']
                print(key)
                result_list.append(key)

                # 파일 내용 읽어오기
                file_obj = s3.get_object(Bucket=bucket_name, Key=key)
                file_content = file_obj['Body'].read()

                # 파일 확장자 확인
                if key.endswith(".pickle"):
                    # 피클 파일을 데이터프레임으로 변환하여 리스트에 추가
                    df = pd.read_pickle(file_content)
                    dataframes.append(df)

    combined_dataframe = pd.concat(dataframes, axis=0, ignore_index=True)

    return combined_dataframe

# 글자 외 기호, 문단 띄어쓰기 제거
def clean_daum_text(data):

    data['기사내용'] = data['기사내용'].apply(lambda x: re.sub(r'[^\s가-힣\d\.\']', '', x))
    data['기사내용'] = data['기사내용'].str.replace('\n', '')
  
    return data

# 전날에 해당되는 데이터만 추출
def recent_daum(data):

    # 'date' 열을 날짜 형식으로 변환
    data['날짜'] = pd.to_datetime(data['날짜'], errors='coerce')

    # 현재 날짜를 얻어옴
    current_date = datetime.datetime.now()

    # 하루 전의 00:00:00 계산
    one_day_ago = current_date - timedelta(days=1)
    one_day_ago = one_day_ago.replace(hour=0, minute=0, second=0, microsecond=0)

    # 최근 데이터만 남기고 나머지 행 삭제
    recent_data = data[data['날짜'] >= one_day_ago]

    return recent_data


# 요약 파이프라인 실행 함수
def daum_summarize_text(text, summarization_model):
    try:
        result = summarization_model(text)
        print(result)
        return result
    except Exception as e:
        print(f"Error in summarizing: {e}")
        return None

# 감성 분석 파이프라인 실행 함수
def daum_sentiment_analysis(text, sentiment_model):
    try:
        result = sentiment_model(text)
        print(result)
        return result
    except Exception as e:
        print(f"Error in summarizing: {e}")
        return None


# 키워드 추출 이전 전처리 함수
def before_kmr(df, content_col):
    temp_list = df[content_col].tolist()
    for i in range(len(temp_list)):
        # 정규식을 사용하여 연속된 공백을 한 개로 대체
        temp_list[i] = re.sub(r'\s+', ' ', temp_list[i])

        # 특수문자 제거
        pattern = re.compile(r'[^ A-Za-z0-9가-힣.]')
        temp_list[i] = pattern.sub('', temp_list[i])

    return temp_list


# komoran으로 명사 추출하는 함수
def extract_nouns_komoran(text, keyword_model):
    nouns = keyword_model.nouns(text)
    return nouns

# 상위 15개 키워드 추출
def extract_nouns(df, column_nm, top_n):
    top_n_nouns_ls = []

    for row in df[column_nm]:
        nouns = [noun for noun in row if len(noun) > 1]
        noun_counts = Counter(nouns)

        # 상위 15개 추출
        top_n_nouns = [noun for noun, count in noun_counts.most_common(top_n)][:top_n]
        top_n_nouns_ls.append(top_n_nouns)
    return top_n_nouns_ls


#다음 뉴스 요약, 감성분석, 키워드추출 메인 실행 함수
def daum_news_pipe(summarization_model, sentiment_model, keyword_model):
    data = get_daum_news()
    data = clean_daum_text(data)
    data = recent_daum(data)

    # '기사내용' 열에 요약 파이프라인 적용하고 결과를 '요약' 열에 저장
    data['요약'] = data['기사내용'].apply(daum_summarize_text, summarization_model = summarization_model)
    data['요약'] = data['요약'].apply(lambda x: x[0]['generated_text'] if isinstance(x, list) and len(x) > 0 and 'generated_text' in x[0] else x)
    data.dropna(subset=['요약'], inplace=True)

    # '요약' 열에 감성분석 파이프라인 적용하고 결과를 '감성분석' 열에 저장
    data['감성분석'] = data['요약'].apply(daum_sentiment_analysis, sentiment_model=sentiment_model)
    data.dropna(subset=['감성분석'], inplace=True)

    # '기사내용' 열에 키워드추출 파이프라인 적용하고 결과를 '키워드_코모란'에 저장 후 상위 15개를 추출하여 '키워드_리스트' 열에 저장
    data['기사내용'] = before_kmr(data, '기사내용')   
    data = data[data['기사내용'] != ' '].reset_index(drop=True) 
    data['키워드_코모란'] = data['기사내용'].apply(extract_nouns_komoran, keyword_model = keyword_model)
    data['키워드_리스트'] = extract_nouns(data, '키워드_코모란', 15)
    data.drop('키워드_코모란', axis=1, inplace=True)


    #클라우드에 업로드 시작
    s3, bucket_name, _ = s3_setting()

    object_nm = str(datetime.datetime.now().strftime('%Y%m%d')) + '/daum_news_crawling/' # 폴더 이름
    pickle_filename = 'daum_news_pipe.pickle'  # 파일 이름
    pickle_buffer = io.BytesIO()
    data.to_pickle(pickle_buffer)  # 피클 파일로 저장
    pickle_buffer.seek(0)

    object_name = object_nm + str(pickle_filename)
    s3.upload_fileobj(pickle_buffer, bucket_name, object_name)  # 클라우드에 업로드
    print(f'Uploaded {pickle_filename} to S3')
