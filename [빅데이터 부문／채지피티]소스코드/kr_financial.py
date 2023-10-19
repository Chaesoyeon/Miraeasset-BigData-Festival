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
from s3_setting import *


#IS데이터 불러오기 (전종목 포괄손익계산서)
def is_fin_data(session, code_list, krx):

    for z, code in enumerate(code_list):
        try: 
            url_test = 'https://comp.fnguide.com/SVO2/asp/SVD_Finance.asp?pGB=1&gicode=%s&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701' % code
            r = session.get(url_test)
            r.encoding='utf-8'
            
            data = pd.read_html(r.text)
            
            IS_temp = data[1]
            IS_temp.index = IS_temp['IFRS(연결)'].values
            IS_temp.drop(['IFRS(연결)','전년동기','전년동기(%)'], inplace=True, axis=1)
            
            for i, name in enumerate(IS_temp.index):
                if '참여한' in name:
                    name = name.strip()
                    name = name.replace('계산에 참여한 계정 펼치기','')
                    name = name.replace(' ','')
                    IS_temp.rename(index = {str(IS_temp.index[i]): str(name)}, inplace=True)
                    
            IS_temp = IS_temp.T      
            IS_temp = IS_temp[['매출액', '매출원가', '매출총이익', '판매비와관리비', '영업이익', '당기순이익']]
            
            IS_temp = IS_temp.reset_index()
            IS_temp.rename(columns={'index': '날짜'}, inplace=True)

            IS_temp.insert(0,'종목코드', code)
            if z == 0:
                IS_data = IS_temp
            else:
                IS_data = pd.concat([IS_data, IS_temp])
        except KeyError as e:
            print(e, code)
        except ValueError as e:
            print(e, code)

    # reset_index
    IS_data = IS_data.reset_index(drop='index')
    # merge
    IS_data = pd.merge(IS_data, krx, on='종목코드', how='inner')

    return IS_data
   

#BS데이터 불러오기 (전종목 재무상태표)
def bs_fin_data(session, code_list, krx):

    for z, code in enumerate(code_list):
        try: 
            url_test = 'https://comp.fnguide.com/SVO2/asp/SVD_Finance.asp?pGB=1&gicode=%s&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701' % code
            r = session.get(url_test)
            r.encoding='utf-8'
            
            data = pd.read_html(r.text)
            
            BS_temp = data[3]
            BS_temp.index = BS_temp['IFRS(연결)'].values
            BS_temp.drop('IFRS(연결)', inplace=True, axis=1)
            
            for i, name in enumerate(BS_temp.index):
                if '참여한' in name:
                    name = name.strip()
                    name = name.replace('계산에 참여한 계정 펼치기','')
                    name = name.replace(' ','')
                    BS_temp.rename(index = {str(BS_temp.index[i]): str(name)}, inplace=True)
                    
            BS_temp = BS_temp.T
            BS_temp = BS_temp[['자산', '부채','자본']]        
            
            BS_temp = BS_temp.reset_index()
            BS_temp.rename(columns={'index': '날짜'}, inplace=True)

            BS_temp.insert(0,'종목코드', code)
            if z == 0:
                BS_data = BS_temp
            else:
                BS_data = pd.concat([BS_data, BS_temp])
        except KeyError as e:
            print(e, code)
        except ValueError as e:
            print(e, code)

    # reset_index
    BS_data = BS_data.reset_index(drop='index')
    # merge
    BS_data = pd.merge(BS_data, krx, on='종목코드', how='inner')

    return BS_data


#CF데이터 불러오기 (전종목 현금흐름표)
def cf_fin_data(session, code_list, krx):

    for z, code in enumerate(code_list):
        try: 
            url_test = 'https://comp.fnguide.com/SVO2/asp/SVD_Finance.asp?pGB=1&gicode=%s&cID=&MenuYn=Y&ReportGB=D&NewMenuID=103&stkGb=701' % code
            r = session.get(url_test)
            r.encoding='utf-8'
            
            data = pd.read_html(r.text)
            
            CF_temp = data[5]
            CF_temp.index = CF_temp['IFRS(연결)'].values
            CF_temp.drop('IFRS(연결)', inplace=True, axis=1)
            
            for i, name in enumerate(CF_temp.index):
                if '참여한' in name:
                    name = name.strip()
                    name = name.replace('계산에 참여한 계정 펼치기','')
                    name = name.replace(' ','')
                    CF_temp.rename(index = {str(CF_temp.index[i]): str(name)}, inplace=True)
                    
            CF_temp = CF_temp.T
            CF_temp = CF_temp[['영업활동으로인한현금흐름','투자활동으로인한현금흐름','재무활동으로인한현금흐름']]
            CF_temp.columns.values[0] = '영업활동'
            CF_temp.columns.values[1] = '투자활동'
            CF_temp.columns.values[2] = '재무활동'
        
            CF_temp = CF_temp.reset_index()
            CF_temp.rename(columns={'index': '날짜'}, inplace=True)

            CF_temp.insert(0,'종목코드', code)
            if z == 0:
                CF_data = CF_temp
            else:
                CF_data = pd.concat([CF_data, CF_temp])
        except KeyError as e:
            print(e, code)
        except ValueError as e:
            print(e, code)

    # reset_index
    CF_data = CF_data.reset_index(drop='index')
    # merge
    CF_data = pd.merge(CF_data, krx, on='종목코드', how='inner')

    return CF_data


#전종목 투자지표 불러오기
def invest_fin_data(session, code_list, krx):

    for z, code in enumerate(code_list):
        try: 
            url_test = 'https://comp.fnguide.com/SVO2/asp/SVD_Invest.asp?pGB=1&gicode=%s&cID=&MenuYn=Y&ReportGB=&NewMenuID=105&stkGb=701' % code
            r = session.get(url_test)
            r.encoding='utf-8'
            
            data = pd.read_html(r.text)
            
            invest = data[3]
            invest.index = invest['IFRS 연결'].values
            invest.drop('IFRS 연결', inplace=True, axis=1)
            
            for i, name in enumerate(invest.index):         
                # 저 글씨 포함하고 있는 애들만 걸리도록 설정
                if '참여한' in name:
                    name = name.strip().replace('계산에 참여한 계정 펼치기','')
                    name = name.replace(' ', '')
                    # index 새로 설정
                    invest.rename(index = {str(invest.index[i]): str(name)}, inplace=True)  
                            
            invest = invest.T
            invest = invest[['PER','PSR','PBR','EV/EBITDA']]
            invest = invest.reset_index()
            invest.rename(columns = {'index': 'date'}, inplace = True)
            invest.insert(0,'종목코드', code)
            if z == 0:
                invest_data = invest
            else:
                invest_data = pd.concat([invest_data, invest])
        except KeyError as e:
            print(e, code)
        except ValueError as e:
            print(e, code)

    # reset_index
    invest_data = invest_data.reset_index(drop='index')
    # merge
    invest_data = pd.merge(invest_data, krx, on='종목코드', how='inner')

    return invest_data

#클라우드에 업로드하기
def kr_fin_upload(ticker, data, s3, bucket_name):

    # 데이터프레임을 파일로 저장
    xlsx_filename = f'{ticker}_재무제표.xlsx'
    xlsx_buffer = BytesIO()
    data.to_excel(xlsx_buffer, index = False)
    xlsx_buffer.seek(0)

    # xlsx 파일 클라우드 업로드
    object_name = str(dt.datetime.now().strftime('%Y%m%d')) + '/financial_kr/' + str(xlsx_filename)
    s3.upload_fileobj(xlsx_buffer, bucket_name, object_name)
    print(f"Uploaded {xlsx_filename} to S3")




#전종목 재무제표 다 불러오기
def get_financial_data():
    session = requests.Session()
    krx, code_list = get_comp_list()
    IS_data = is_fin_data(session, code_list, krx)
    BS_data = bs_fin_data(session, code_list, krx)
    CF_data = cf_fin_data(session, code_list, krx)
    invest_data = invest_fin_data(session, code_list, krx)
    
    s3, bucket_name, _ = s3_setting()
    # 클라우드 폴더 생성
    object_name = str(dt.datetime.now().strftime('%Y%m%d')) + '/financial_kr/'
    s3.put_object(Bucket=bucket_name, Key=object_name)
    
    
    kr_fin_upload('IS', IS_data, s3, bucket_name)
    kr_fin_upload('BS', BS_data, s3, bucket_name)
    kr_fin_upload('CF', CF_data, s3, bucket_name)
    kr_fin_upload('Invest', invest_data, s3, bucket_name)

    return IS_data, BS_data, CF_data, invest_data
