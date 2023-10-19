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
from s3_setting import *


#미국 뉴스 데이터 클라우드에서 불러오기(취합)
def get_usa_news():
    s3, bucket_name, _ = s3_setting()
    folder_prefix = str(datetime.datetime.now().strftime('%Y%m%d')) + '/usa_news_crawling/'

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
                result_list.append(key)

                # 파일 내용 읽어오기
                file_obj = s3.get_object(Bucket=bucket_name, Key=key)
                file_content = file_obj['Body'].read()

                # 파일 확장자 확인
                if key.endswith(".pickle"):
                    # 엑셀 파일을 데이터프레임으로 변환하여 리스트에 추가
                    df = pd.read_pickle(file_content)
                    dataframes.append(df)
        
    combined_dataframe = pd.concat(dataframes, axis=0, ignore_index=True)
  
    #미국 기업리스트 불러오기
    comp_list = get_uscomp_list()
    # combined_datrame에서 '검색어' 열을 기준으로 'comp_list'와 병합 (merge)
    combined_dataframe = combined_dataframe.merge(comp_list[['company2', 'Ticker']], left_on='검색어', right_on='company2', how='left')

    # 필요 없는 삭제
    combined_dataframe.drop(columns=['company2'], inplace=True)
    combined_dataframe.drop(columns=['언론사'], inplace=True)

    return combined_dataframe

#전날 하루의 데이터만 불러오기
def usa_news_recent_data(data):

    # 'date' 열을 날짜 형식으로 변환
    data['날짜'] = pd.to_datetime(data['날짜'], errors='coerce')

    # 현재 날짜를 얻어옴
    current_date = datetime.datetime.now() - timedelta(days=1)

    # 하루 전의 00:00:00 계산
    one_day_ago = current_date - timedelta(days=1)
    one_day_ago = one_day_ago.replace(hour=0, minute=0, second=0, microsecond=0)

    # 최근 데이터만 남기고 나머지 행 삭제
    recent_data = data[data['날짜'] >= one_day_ago]

    return recent_data



# 기사 머리글 지우기
def clean_ustext(data):

    # '-' 문자가 첫 번째로 등장하는 인덱스를 찾습니다.
    first_dash_index = data['기사내용'].str.find('-')

    # '-' 이 발견된 행만 처리합니다.
    data['기사내용'] = data.apply(lambda row: row['기사내용'][int(first_dash_index[row.name])+1:] if first_dash_index[row.name] >= 0 else row['기사내용'], axis=1)

    data.dropna(inplace=True)
    data.drop_duplicates(subset=['기사내용'], keep='first', inplace=True)

    return data


# 키워드 추출 파이프라인 실행 함수
def us_keyword(text, keyword_model):
    try:
        result = keyword_model(text)
        print(result)
        return result
    except Exception as e:
        print(f"Error in keyword: {e}")
        return None

    
# 키워드 추출 후 단어만 추출하는 함수
def extract_words(keyword_list):
    return [item['word'] for item in keyword_list]


# 'Ġ' 문자를 모든 문자열에서 제거하는 함수
def remove_special_characters(text_list):
    return [text.replace('Ġ', '') for text in text_list]


# 요약 파이프라인 실행 함수
def us_summarize_text(text, summarization_model):
    try:
        if len(text) > 2000:
            text = text[:2000]
        result = summarization_model(text)
        print(result)
        return result
    except Exception as e:
        print(f"Error in summarizing: {e}")
        return None

    
# 감성 분석 파이프라인 실행 함수
def us_sentiment_analysis(text, sentiment_model):
    try:
        result = sentiment_model(text)
        print(result)
        return result
    except Exception as e:
        print(f"Error in sentiment: {e}")
        return None


#main 실행함수
def usa_news_pipe(summarization_model, sentiment_model, keyword_model):
    data = get_usa_news()
    data = usa_news_recent_data(data)
    data = clean_ustext(data)
 

    # '기사내용' 열에 요약 파이프라인 적용하고 결과를 '요약' 열에 저장
    data['요약'] = data['기사내용'].apply(us_summarize_text, summarization_model = summarization_model)
    data['요약'] = data['요약'].apply(lambda x: x[0]['summary_text'] if isinstance(x, list) and len(x) > 0 and 'summary_text' in x[0] else x)
    data.dropna(subset=['요약'], inplace=True)

    # '요약' 열에 감성분석 파이프라인 적용하고 결과를 '감성분석' 열에 저장
    data['감성분석'] = data['요약'].apply(us_sentiment_analysis, sentiment_model=sentiment_model)
    data.dropna(subset=['감성분석'], inplace=True)
    
    # '기사내용' 열에 키워드 추출 적용하고 결과를 '키워드' 열에 저장 후 단어만 다시 추출하여 '키워드_리스트' 열에 저장
    data['키워드'] = data['기사내용'].apply(us_keyword, keyword_model = keyword_model)
    data.dropna(subset=['키워드'], inplace=True)
    data['키워드_리스트'] = data['키워드'].apply(extract_words)
    data['키워드_리스트'] = data['키워드_리스트'].apply(remove_special_characters)


    #클라우드에 업로드 시작
    s3, bucket_name, _ = s3_setting()

    object_nm = str(datetime.datetime.now().strftime('%Y%m%d')) + '/usa_news_crawling/' # 폴더 이름
    pickle_filename = 'usa_news_pipe.pickle'  # 파일 이름
    pickle_buffer = io.BytesIO()
    data.to_pickle(pickle_buffer)  # 피클 파일로 저장
    pickle_buffer.seek(0)

    object_name = object_nm + str(pickle_filename)
    s3.upload_fileobj(pickle_buffer, bucket_name, object_name)  # 클라우드에 업로드
    print(f'Uploaded {pickle_filename} to S3')
