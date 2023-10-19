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


#클라우드에서 국내 주가데이터 불러오기
def krchart_download_stock_data(s3, bucket_name, ticker):
    
    file_name = f'{ticker}_주가데이터.xlsx'
    object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/kr_stock_crawling/' + str(file_name)

    # S3에서 파일 내용을 읽어옴
    response = s3.get_object(Bucket=bucket_name, Key=object_name)
    content = response['Body'].read()

    # 읽어온 엑셀 파일을 데이터프레임으로 변환
    df = pd.read_excel(content)

    return df


# 6개월로 기간 줄이기
def krchart_six_month(df):
    df['Date'] = pd.to_datetime(df['Date'])
    
    recent_date = df['Date'].max()
    
    start_date = recent_date - pd.DateOffset(months = 6)
    start_date = start_date.replace(microsecond = 0, nanosecond = 0)
    
    df = df[(df['Date'] >= start_date) & (df['Date'] <= recent_date)]
    
    return df


#변동이 큰 구간 4개 추출하기(start_date, end_date, duration, change_average 계산)
def krchart_process_dataframe(df):
    selected_rows = []
    consecutive_count_up = 0
    consecutive_count_down = 0
    start_date_up = None
    start_date_down = None
    change_sum_up = 0
    change_sum_down = 0

    for index, row in df.iterrows():
        if row['Change'] >= 0:  # 양수에서 음수로 바뀐 경우
            if consecutive_count_up == 0:
                start_date_up = row['Date']  # 양수에서 음수로 바뀐 날짜 기록
            consecutive_count_up += 1
            change_sum_up += row['Change']
        elif row['Change'] <= 0:  # 음수에서 양수로 바뀐 경우
            if consecutive_count_up > 0:
                # 이전 row의 값을 사용하여 'End Date' 설정
                prev_row = df.loc[index - 1]
                selected_rows.append({
                    'Start Date': start_date_up,
                    'End Date': prev_row['Date'],
                    'Duration': consecutive_count_up,  # 바뀐 이후 다시 양수로 바뀌는 시점까지의 기간
                    'Change Average': change_sum_up / (consecutive_count_up)  # Duration 동안의 Change 평균
                })
            consecutive_count_up = 0
            change_sum_up = 0

        if row['Change'] <= 0:  # 음수에서 양수로 바뀐 경우
            if consecutive_count_down == 0:
                start_date_down = row['Date']  # 음수에서 양수로 바뀐 날짜 기록
            consecutive_count_down += 1
            change_sum_down += row['Change']
        elif row['Change'] >= 0:  # 양수에서 음수로 바뀐 경우
            if consecutive_count_down > 0:
                # 이전 row의 값을 사용하여 'End Date' 설정
                prev_row = df.loc[index - 1]
                selected_rows.append({
                    'Start Date': start_date_down,
                    'End Date': prev_row['Date'],
                    'Duration': consecutive_count_down,  # 바뀐 이후 다시 음수로 바뀌는 시점까지의 기간
                    'Change Average': change_sum_down / (consecutive_count_down)  # Duration 동안의 Change 평균
                })
            consecutive_count_down = 0
            change_sum_down = 0

    if selected_rows:
        selected_df = pd.DataFrame(selected_rows)
        selected_df = selected_df[selected_df['Duration'] > 1]
        s_df = selected_df.copy()
        sorted_index = s_df['Change Average'].abs().sort_values(ascending = False).index
        sorted_selected_df = selected_df.loc[sorted_index].head(4)
        return sorted_selected_df
    else:
        return pd.DataFrame()  # 빈 데이터프레임 반환

#클라우드에 업로드
def krchart_upload(ticker, data, s3, bucket_name):

    # 데이터프레임을 파일로 저장
    xlsx_filename = f'{ticker}_차트분석.xlsx'
    xlsx_buffer = io.BytesIO()
    data.to_excel(xlsx_buffer, index = False)
    xlsx_buffer.seek(0)

    # xlsx 파일 클라우드 업로드
    object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/stock_chart_kr/' + str(xlsx_filename)
    s3.upload_fileobj(xlsx_buffer, bucket_name, object_name)
    print(f"Uploaded {xlsx_filename} to S3")


#main 실행함수
def krchart_main():

    s3, bucket_name, _ = s3_setting()

    comp_df,_ = get_comp_list()
    kr_company_list = comp_df.기업

    # 클라우드 폴더 생성
    object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/stock_chart_kr/'
    s3.put_object(Bucket=bucket_name, Key=object_name)

    for company in kr_company_list:
        ticker = company

        st_data = krchart_download_stock_data(s3, bucket_name, ticker)
        st_data = krchart_six_month(st_data)

        result_df = krchart_process_dataframe(st_data)
        result_df['기업'] = ticker

        # 주가 데이터 수집 및 파일로 저장
        krchart_upload(ticker, result_df, s3, bucket_name)