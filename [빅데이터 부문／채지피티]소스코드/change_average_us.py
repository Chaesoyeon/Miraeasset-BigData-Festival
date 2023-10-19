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

#미국 주가데이터 클라우드에서 불러오기
def us_avg_download_stock_data(s3, bucket_name, ticker):
    
    file_name = f'{ticker}_주가데이터.xlsx'
    object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/usa_stock_crawling/' + str(file_name)

    # S3에서 파일 내용을 읽어옴
    response = s3.get_object(Bucket=bucket_name, Key=object_name)
    content = response['Body'].read()

    # 읽어온 엑셀 파일을 데이터프레임으로 변환
    df = pd.read_excel(content)

    return df

#평균 등략률 계산(3개월)
def us_average_change(data):
    today = datetime.datetime.today()
    
    one_month = data[(today - data['Date']).dt.days <= 30]['Change'].mean()
    two_month = data[(today - data['Date']).dt.days <= 60]['Change'].mean()
    three_month = data[(today - data['Date']).dt.days <= 90]['Change'].mean()

    result_df = pd.DataFrame({'기간' : ['1개월', '2개월', '3개월'],
                              '평균 등락률' : [one_month, two_month, three_month]})
    
    return result_df

#클라우드에 업로드
def usavg_upload(ticker, data, s3, bucket_name):

    # 데이터프레임을 파일로 저장
    xlsx_filename = f'{ticker}_평균등락률.xlsx'
    xlsx_buffer = io.BytesIO()
    data.to_excel(xlsx_buffer, index = False)
    xlsx_buffer.seek(0)

    # xlsx 파일 클라우드 업로드
    object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/change_average_usa/' + str(xlsx_filename)
    s3.upload_fileobj(xlsx_buffer, bucket_name, object_name)
    print(f"Uploaded {xlsx_filename} to S3")

#main 실행 함수
def usavg_main():

    s3, bucket_name, _ = s3_setting()
    us_company_list = get_uscomp_list()
    # 클라우드 폴더 생성
    object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/stock_chart_usa/'
    s3.put_object(Bucket=bucket_name, Key=object_name)

    for index, row in us_company_list.iterrows():
        ticker = row['Ticker']

        st_data = us_avg_download_stock_data(s3, bucket_name, ticker)
        result_df = us_average_change(st_data)

        result_df['Ticker'] = ticker

        # 주가 데이터 수집 및 파일로 저장
        usavg_upload(ticker, result_df, s3, bucket_name)