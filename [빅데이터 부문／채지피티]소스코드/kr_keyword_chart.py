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
from s3_setting import *


# 국내 차트분석 데이터 클라우드에서 불러오기
def kr_keyword_chart_data(s3, bucket_name, ticker):

    xlsx_filename = f'{ticker}_차트분석.xlsx'
    object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/stock_chart_kr/' + str(xlsx_filename)

    # S3에서 파일 내용을 읽어옴
    response = s3.get_object(Bucket=bucket_name, Key=object_name)
    content = response['Body'].read()

    # 읽어온 엑셀 파일을 데이터프레임으로 변환
    df = pd.read_excel(content, index_col=0)
    df.reset_index(inplace=True, drop=True)

    return df


# 국내뉴스_요약감성키워드 데이터 데이터 클라우드에서 불러오기
def keyword_kr_pipe_data(s3, bucket_name):

    #국내뉴스_요약감성키워드 데이터 불러오기
    pickle_filename = 'daum_news_pipe.pickle'
    pk_object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/daum_news_crawling/' + str(pickle_filename)

    # S3에서 파일 내용을 읽어옴
    response = s3.get_object(Bucket=bucket_name, Key=pk_object_name)
    content = response['Body'].read()

    df = pd.read_pickle(content)

    return df


# 4구간에 대해 키워드, 등장 시점, 요약텍스트, 해당 기사 링크 저장
def kr_keyword_chart_make_df(date, df):

    date['키워드'] = ''
    date['키워드 등장 시점'] = ''
    date['요약'] = ''
    date['링크'] = ''
    for i in range(len(date)):
        start_date = pd.to_datetime(date.iloc[i,0])
        end_date = pd.to_datetime(date.iloc[i,1])

        df['날짜'] = pd.to_datetime(df['날짜'])

        filtered_df = df[(df['날짜'] >= start_date) & (df['날짜'] <= end_date)]

        # 키워드 추출 및 빈도 계산
        all_keywords = []
        for word_list in filtered_df['키워드_리스트']:
            all_keywords.extend(word_list)

        keyword_counts = Counter(all_keywords)

        # 상위 3개 키워드 추출
        top_keywords = keyword_counts.most_common(3)
        keywords_only = [keyword for keyword, _ in top_keywords]
        if keywords_only != []:
            first_keyword = keywords_only[0]

            filtered_dates = filtered_df[filtered_df['키워드_리스트'].apply(lambda x: first_keyword in x)]['날짜']
            # 가장 오래된 발행일 찾기
            oldest_date = min(filtered_dates)

            # 가장 오래된 발행일에 해당하는 sum_text 가져오기
            oldest_sum_text = filtered_df[filtered_df['날짜'] == oldest_date]['요약'].values[0]

            # 가장 오래된 발행일에 해당하는 링크 가져오기
            oldest_link = filtered_df[filtered_df['날짜'] == oldest_date]['링크'].values[0]

            date.at[i, '키워드']  = keywords_only
            date.at[i, '요약']  = oldest_sum_text
            date.at[i, '키워드 등장 시점'] = oldest_date
            date.at[i, '링크']  = oldest_link


    return date


#클라우드에 업로드
def keyword_kr_upload(ticker, data, s3, bucket_name):

    # 데이터프레임을 파일로 저장
    xlsx_filename = f'{ticker}_키워드차트.xlsx'
    xlsx_buffer = io.BytesIO()
    data.to_excel(xlsx_buffer, index = False)
    xlsx_buffer.seek(0)

    # xlsx 파일 클라우드 업로드
    object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/keyword_chart_kr/' + str(xlsx_filename)
    s3.upload_fileobj(xlsx_buffer, bucket_name, object_name)
    print(f"Uploaded {xlsx_filename} to S3")


#main 실행 함수
def keyword_chart_kr_main():

    s3, bucket_name, _ = s3_setting()

    comp_df,_ = get_comp_list()
    kr_company_list = comp_df.기업

    # 클라우드 폴더 생성
    object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/keyword_chart_kr/'
    s3.put_object(Bucket=bucket_name, Key=object_name)

    df = keyword_kr_pipe_data(s3, bucket_name)

    for company in kr_company_list:
        ticker = company

        date = kr_keyword_chart_data(s3, bucket_name, ticker)

        df = df[df['검색어']==ticker]
        date = kr_keyword_chart_make_df(date, df)

        # 주가 데이터 수집 및 파일로 저장
        keyword_kr_upload(ticker, date, s3, bucket_name)