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
import boto3
from s3_setting import *



# 종목코드뒤에 시장구분 따라 알파벳 붙이기
def change_usticker(data):
    data['Ticker'] = data['Ticker'].str.replace('.A', '-A', regex = False).str.replace('.B', '-B', regex = False)
    return data


#데이터 불러와서 클라우드에 업로드하기
def collect_and_upload_usstock_data(tickers, start_date, end_date, s3_client, bucket_name, yf_usa, usa):
    failed_ls = []
    
    for ticker in tickers:
        data = yf.download(ticker, start=start_date, end=end_date)
        data['Change'] = data['Adj Close'].pct_change() * 100  # 등락률 계산
        data = data.drop('Adj Close', axis = 1)
        data['Ticker'] = ticker
        data = data[1:]  # 첫 번째 행 제거
        
        if data['Open'].eq(0).all():
            print(f"Data for {ticker} has all 'Open' values as 0. Skipping upload.")
            failed_ls.append(ticker)
            continue
            
        index = yf_usa[yf_usa['Ticker'] == ticker].index.values[0]
        title_ticker = usa.at[index, 'Ticker']

        # 데이터프레임을 파일로 저장
        xlsx_filename = f'{title_ticker}_주가데이터.xlsx'
        xlsx_buffer = io.BytesIO()
        data.to_excel(xlsx_buffer)
        xlsx_buffer.seek(0)

        # xlsx 파일 클라우드 업로드
        object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/usa_stock_crawling/' + str(xlsx_filename)
        s3_client.upload_fileobj(xlsx_buffer, bucket_name, object_name)
        print(f"Uploaded {xlsx_filename} to S3")
        
    print('Failed tickers: ', failed_ls)
    return failed_ls


#main 실행 함수
def usa_stock_crawling():
    # AWS S3 설정
    s3, bucket_name, _ = s3_setting()
    
    # 리스트 불러오고 형태 변경
    usa = get_uscomp_list()
    yf_usa = change_usticker(usa)

    # 티커 리스트 생성
    usa_tickers = yf_usa.iloc[:,1].unique()

    # 오늘 날짜로부터 하루 전의 날짜 계산
    end_date = datetime.datetime.today()
    ## end에 설정한 일자의 전일자까지 조회됨.
    start_date = (datetime.datetime.today() - timedelta(days=1 + 1))
    ## 하루+1일전 데이터를 수집해야 하루 전날까지의 등락률을 계산할 수 있음.

    # 클라우드 폴더 생성
    object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/usa_stock_crawling/'
    s3.put_object(Bucket=bucket_name, Key=object_name)

    # 주가 데이터 수집 및 파일로 저장
    collect_and_upload_usstock_data(usa_tickers, start_date, end_date, s3, bucket_name, yf_usa, usa)

