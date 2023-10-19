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
import datetime as dt
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
def kr_change_ticker(row):
    if 'KOSDAQ' in row['시장구분']:
        return row['종목코드'].replace("'", '') + '.KQ'
    elif 'KOSPI' in row['시장구분']:
        return row['종목코드'].replace("'", '') + '.KS'
    else:
        return row['종목코드'].replace("'", '')

    
#국내 종목코드에서 'A' 빼고 숫자만 남기기, 시장구분 따라 알파벳 붙이기
def get_krticker_list(data):
    data['종목코드'] = data['종목코드'].str[1:]
    new_data = data.copy()

    new_data['종목코드'] = new_data.apply(kr_change_ticker, axis = 1)

    return data, new_data


#국내 주가 데이터 크롤링
def collect_krstock_data(ticker, start_date, end_date, korea, new_korea):
    # KONEX의 경우 pykrx에 데이터가 있음
    if new_korea.loc[new_korea['종목코드'] == ticker, '시장구분'].values[0] == 'KONEX':
        data = stock.get_market_ohlcv_by_date(fromdate=start_date.strftime('%Y%m%d'), todate=end_date.strftime('%Y%m%d'), ticker=ticker)
        data.reset_index(inplace = True)
        data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change']
        data['Ticker'] = ticker
        result = data[1:]  # 첫 번째 행 제거 - 첫 번째 행은 2년 전 해당일의 등락률 계산을 위해서만 사용
    else:
        try:
            data = yf.download(ticker, start=start_date, end=end_date)
            if not data.empty and 'Open' in data.columns:
                data['Change'] = data['Adj Close'].pct_change() * 100  # 등락률 계산
                data = data.drop('Adj Close', axis = 1)
                data['Ticker'] = ticker.split('.')[0]  # ticker 값에 '.KS' 부분 제거
                result = data[1:]  # 첫 번째 행 제거
            else:
                raise Exception("Data from yf.download is empty or missing 'Adj Close' column.")
        except Exception as e:
            ticker_new = ticker.split('.')[0]
            print(f"Error for {ticker}: {e}")
            data = stock.get_market_ohlcv_by_date(fromdate=start_date.strftime('%Y%m%d'), todate=end_date.strftime('%Y%m%d'), ticker=ticker_new)
            data.reset_index(inplace = True)
            data.columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Change']
            data['Ticker'] = ticker_new
            result = data[1:]  # 첫 번째 행 제거 - 첫 번째 행은 2년 전 해당일의 등락률 계산을 위해서만 사용

    return result


#클라우드에 업로드
def kr_stock_upload(tickers, start_date, end_date, s3_client, bucket_name, korea, new_korea):
    failed_ls = []
    
    for ticker in tickers:
        data = collect_krstock_data(ticker, start_date, end_date, korea, new_korea)
        
        # '시가' 열의 값이 모두 0인지 확인
        if data['Open'].eq(0).all():
            print(f"Data for {ticker} has all 'Open' values as 0. Skipping upload.")
            failed_ls.append(ticker)
            continue
        
        # 기업명 가져오기
        index = new_korea[new_korea['종목코드'] == ticker].index.values[0]
        company_name = korea.at[index, '기업']
        
        # 데이터프레임을 파일로 저장
        xlsx_filename = f'{company_name}_주가데이터.xlsx'
        xlsx_buffer = io.BytesIO()
        data.to_excel(xlsx_buffer)
        xlsx_buffer.seek(0)
        
        # xlsx 파일 클라우드 업로드
        object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/kr_stock_crawling/' + str(xlsx_filename)
        s3_client.upload_fileobj(xlsx_buffer, bucket_name, object_name)
        print(f"Uploaded {xlsx_filename} to S3")
        
    print('Failed tickers: ', failed_ls)


#main 실행 함수
def get_stock_kr_main():
    #S3 설정
    s3, bucket_name, _ = s3_setting()
    
    # 티커 리스트 생성
    df, _ = get_comp_list()
    korea, new_korea = get_krticker_list(df)
    korea_tickers = new_korea.iloc[:, 2].unique()
    
    # 오늘 날짜로부터 하루 전의 날짜 계산
    end_date = datetime.datetime.today()
    start_date = datetime.datetime.today() - timedelta(days=1 + 1)
    

    # 클라우드 폴더 생성
    object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/kr_stock_crawling/'
    s3.put_object(Bucket=bucket_name, Key=object_name)

    # 주가 데이터 수집 및 파일로 저장
    kr_stock_upload(korea_tickers, start_date, end_date, s3, bucket_name, korea, new_korea)

