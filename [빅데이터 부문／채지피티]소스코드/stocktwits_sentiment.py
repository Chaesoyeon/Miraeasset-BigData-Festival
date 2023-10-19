import boto3
import pandas as pd
import numpy as np
import pickle
import os
import datetime
from datetime import timedelta
import re
from transformers import pipeline
from s3_setting import *

#미국 종목토론방 클라우드에서 불러오기
def stocktwits_data():
    s3, bucket_name, _ = s3_setting()
    folder_prefix = str(datetime.datetime.now().strftime('%Y%m%d')) + '/stocktwits_crawling/'

    result_list = []  # 모든 객체를 저장할 리스트
    paginator = s3.get_paginator('list_objects')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=folder_prefix)

    prefix_length = len("20230905/stocktwits_crawling/")
    suffix_length = len("_20230903.pickle")

    combined_dataframe = []
    dataframes = []
    for page in page_iterator:
        if 'Contents' in page:
            contents_list = page['Contents']
            for content in contents_list:
                key = content['Key']
                name = key[prefix_length:-suffix_length]
                result_list.append(key)

                # 파일 내용 읽어오기
                file_obj = s3.get_object(Bucket=bucket_name, Key=key)
                file_content = file_obj['Body'].read()

                # 파일 확장자 확인
                if key.endswith(".pickle"):
                    # 피클 파일을 데이터프레임으로 변환하여 리스트에 추가
                    df = pd.read_pickle(file_content)
                    df['name'] = name
                    dataframes.append(df)

    combined_dataframe = pd.concat(dataframes, axis=0, ignore_index=True)
    return combined_dataframe


#날짜 조정(하루 전날 데이터 추출)
def stocktwits_recent_data(data):

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


#데이터 전처리
def stocktwits_preprocess(data):

    # '내용' 열에서 영어와 '$'를 제외한 글자들 제거
    data['내용'] = data['내용'].apply(lambda x: re.sub(r'[^\sa-zA-Z\$]', '', x))

    # '$' 다음에 띄어쓰기가 나올 때까지 나오는 글자 추출하여 새로운 열 추가
    data['관련기업'] = data['내용'].apply(lambda x: re.findall(r'\$[^\s]+', x))

    # '$' 다음에 띄어쓰기가 나올 때까지 나온 글자를 문자열로 변환
    data['관련기업'] = data['관련기업'].apply(lambda x: ' '.join(x) if x else '')

    # '내용' 열에서 '$' 다음에 띄어쓰기가 나올 때까지 나오는 글자 제거
    data['기사내용'] = data['기사내용'].apply(lambda x: re.sub(r'\$[^\s]+', '', x))

    data.reset_index(drop=True, inplace=True)
    
    return data


# 감성 분석 파이프라인 실행 함수
def stock_sentiment_analysis(text, sentiment_model):
    try:
        result = sentiment_model(text)
        print(result)
        return result
    except Exception as e:
        print(f"Error in summarizing: {e}")
        return None
    
    
#main 실행함수
def stocktwit_sentiment(sentiment_model):
    data = stocktwits_data()
    data = stocktwits_recent_data(data)
    data = stocktwits_preprocess(data)

    # '기사내용' 열에 감성분석 파이프라인 적용하고 결과를 '감성분석' 열에 저장
    data['감성분석'] = data['내용'].apply(stock_sentiment_analysis, sentiment_model=sentiment_model)
    data.dropna(subset=['감성분석'], inplace=True)

    #클라우드에 업로드 시작
    s3, bucket_name, _ = s3_setting()

    object_nm = str(datetime.datetime.now().strftime('%Y%m%d')) + '/stocktwits_crawling/' # 폴더 이름
    pickle_filename = 'stock_twits_sentimnt.pickle'  # 파일 이름
    pickle_buffer = io.BytesIO()
    data.to_pickle(pickle_buffer)  # 피클 파일로 저장
    pickle_buffer.seek(0)

    object_name = object_nm + str(pickle_filename)
    s3.upload_fileobj(pickle_buffer, bucket_name, object_name)  # 클라우드에 업로드
    print(f'Uploaded {pickle_filename} to S3')
