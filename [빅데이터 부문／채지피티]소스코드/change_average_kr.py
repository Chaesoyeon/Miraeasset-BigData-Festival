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


#국내 주가데이터 클라우드에서 불러오기 
def kravg_download_stock_data(s3, bucket_name, ticker):
    
    file_name = f'{ticker}_주가데이터.xlsx'
    object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/kr_stock_crawling/' + str(file_name)

    # S3에서 파일 내용을 읽어옴
    response = s3.get_object(Bucket=bucket_name, Key=object_name)
    content = response['Body'].read()

    # 읽어온 엑셀 파일을 데이터프레임으로 변환
    df = pd.read_excel(content)

    return df

#평균 등락률 계산(3개월)
def kr_average_change(data):
    today = datetime.datetime.today()
    
    one_month = data[(today - data['Date']).dt.days <= 30]['Change'].mean()
    two_month = data[(today - data['Date']).dt.days <= 60]['Change'].mean()
    three_month = data[(today - data['Date']).dt.days <= 90]['Change'].mean()

    result_df = pd.DataFrame({'기간' : ['1개월', '2개월', '3개월'],
                              '평균 등락률' : [one_month, two_month, three_month]})
    
    return result_df


#클라우드에 없로드
def kravg_upload(ticker, data, s3, bucket_name):

    # 데이터프레임을 파일로 저장
    xlsx_filename = f'{ticker}_평균등락률.xlsx'
    xlsx_buffer = io.BytesIO()
    data.to_excel(xlsx_buffer, index = False)
    xlsx_buffer.seek(0)

    # xlsx 파일 클라우드 업로드
    object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/change_average_kr/' + str(xlsx_filename)
    s3.upload_fileobj(xlsx_buffer, bucket_name, object_name)
    print(f"Uploaded {xlsx_filename} to S3")

#main 실행 함수
def kravg_main():

    s3, bucket_name, _ = s3_setting()

    comp_df,_ = get_comp_list()
    kr_company_list = comp_df.기업

    # 클라우드 폴더 생성
    object_name = str(datetime.datetime.now().strftime('%Y%m%d')) + '/change_average_kr/'
    s3.put_object(Bucket=bucket_name, Key=object_name)

    for company in kr_company_list:
        ticker = company

        st_data = kravg_download_stock_data(s3, bucket_name, ticker)
        result_df = kr_average_change(st_data)

        result_df['기업'] = ticker

        # 주가 데이터 수집 및 파일로 저장
        kravg_upload(ticker, result_df, s3, bucket_name)