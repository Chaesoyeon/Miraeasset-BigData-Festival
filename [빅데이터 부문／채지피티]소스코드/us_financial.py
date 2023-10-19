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
from s3_setting import *


#IS데이터 불러오기 (전종목 포괄손익계산서)
def is_usfin_data(df):

    result_is = pd.DataFrame()

    # 각 주식 심볼에 대해 데이터 가져오기
    for index, row in df.iterrows():
        symbol = row['Ticker']  # 엑셀 파일에 있는 주식 심볼 컬럼명에 맞게 설정
        
        try:
            data = Ticker(symbol)
            data1 = data.income_statement(frequency='q')
            data1.reset_index(inplace=True)
            
            # 결과 데이터 프레임에 추가
            result_is = result_is.append(data1)
        
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")

    return result_is
   

#BS데이터 불러오기 (전종목 재무상태표)
def bs_usfin_data(df):

    result_bs = pd.DataFrame()

    # 각 주식 심볼에 대해 데이터 가져오기
    for index, row in df.iterrows():
        symbol = row['Ticker']  # 엑셀 파일에 있는 주식 심볼 컬럼명에 맞게 설정
        
        try:
            data = Ticker(symbol)
            data2 = data.balance_sheet(frequency='q')
            data2.reset_index(inplace=True)
            
            # 결과 데이터 프레임에 추가
            result_bs = result_bs.append(data2)
        
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")

    return result_bs


#CF데이터 불러오기 (전종목 현금흐름표)
def cf_usfin_data(df):

    result_cf = pd.DataFrame()

    # 각 주식 심볼에 대해 데이터 가져오기
    for index, row in df.iterrows():
        symbol = row['Ticker']  # 엑셀 파일에 있는 주식 심볼 컬럼명에 맞게 설정
        
        try:
            data = Ticker(symbol)
            data3 = data.cash_flow(frequency='q')
            data3.reset_index(inplace=True)

            # 결과 데이터 프레임에 추가
            result_cf = result_cf.append(data3)
        
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")

    return result_cf

# valuation measures 데이터 불러오기
def vm_usfin_data(df):

    result_vm = pd.DataFrame()

    # 각 주식 심볼에 대해 데이터 가져오기
    for index, row in df.iterrows():
        symbol = row['Ticker']  # 엑셀 파일에 있는 주식 심볼 컬럼명에 맞게 설정
        
        try:
            data = Ticker(symbol)
            data4 = data.valuation_measures
            data4.reset_index(inplace=True)
            
            # 결과 데이터 프레임에 추가
            result_vm = result_vm.append(data4)
        
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")

    return result_vm


#재무제표 전체항목 불러오기
def us_all_financial_data(df):
        
    result_all = pd.DataFrame()

    # 각 주식 심볼에 대해 데이터 가져오기
    for index, row in df.iterrows():
        symbol = row['Ticker']  # 엑셀 파일에 있는 주식 심볼 컬럼명에 맞게 설정
        
        try:
            data = Ticker(symbol)
            data_all = data.all_financial_data(frequency='q')
            data_all.reset_index(inplace=True)

            # 결과 데이터 프레임에 추가
            result_all = result_all.append(data_all)
        
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")

    return result_all



# 클라우드에 업로드
def us_fin_upload(ticker, data, s3, bucket_name):

    # 데이터프레임을 파일로 저장
    xlsx_filename = f'{ticker}_재무제표.xlsx'
    xlsx_buffer = io.BytesIO()
    data.to_excel(xlsx_buffer, index = False)
    xlsx_buffer.seek(0)

    # xlsx 파일 클라우드 업로드
    object_name = str(dt.datetime.now().strftime('%Y%m%d')) + '/financial_us/' + str(xlsx_filename)
    s3.upload_fileobj(xlsx_buffer, bucket_name, object_name)
    print(f"Uploaded {xlsx_filename} to S3")


    
#전종목 재무제표 다 불러오기
def get_us_financial_data():
    df = get_uscomp_list()
    IS_data = is_usfin_data(df)
    BS_data = bs_usfin_data(df)
    CF_data = cf_usfin_data(df)
    VM_data = vm_usfin_data(df)
    all_data = us_all_financial_data(df)
    
    s3, bucket_name, _ = s3_setting()
    # 클라우드 폴더 생성
    object_name = str(dt.datetime.now().strftime('%Y%m%d')) + '/financial_us/'
    s3.put_object(Bucket=bucket_name, Key=object_name)
    
    
    us_fin_upload('IS', IS_data, s3, bucket_name)
    us_fin_upload('BS', BS_data, s3, bucket_name)
    us_fin_upload('CF', CF_data, s3, bucket_name)
    us_fin_upload('VM', VM_data, s3, bucket_name)
    us_fin_upload('all_data', all_data, s3, bucket_name)

    return IS_data, BS_data, CF_data, VM_data, all_data