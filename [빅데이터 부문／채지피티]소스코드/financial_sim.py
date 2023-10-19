import requests
from bs4 import BeautifulSoup
import os
import datetime as dt
import pandas as pd
from datetime import timedelta
import re
from io import BytesIO
from s3_setting import *


#국내 재무제표 데이터 불러오기
def kr_financial_data_for_sim(fin_date, s3, bucket_name):

    is_filename = 'IS_재무제표.xlsx'
    bs_filename = 'BS_재무제표.xlsx'
    cf_filename = 'CF_재무제표.xlsx'
    invest_filename = 'Invest_재무제표.xlsx'

    is_object_name = str(fin_date) + '/financial_kr/' + str(is_filename)
    bs_object_name = str(fin_date) + '/financial_kr/' + str(bs_filename)
    cf_object_name = str(fin_date) + '/financial_kr/' + str(cf_filename)
    invest_object_name = str(fin_date) + '/financial_kr/' + str(invest_filename)

    # S3에서 파일 내용을 읽어옴
    is_response = s3.get_object(Bucket=bucket_name, Key=is_object_name)
    is_content = is_response['Body'].read()

    bs_response = s3.get_object(Bucket=bucket_name, Key=bs_object_name)
    bs_content = bs_response['Body'].read()

    cf_response = s3.get_object(Bucket=bucket_name, Key=cf_object_name)
    cf_content = cf_response['Body'].read()

    invest_response = s3.get_object(Bucket=bucket_name, Key=invest_object_name)
    invest_content = invest_response['Body'].read()


    # 읽어온 엑셀 파일을 데이터프레임으로 변환
    is_df = pd.read_excel(is_content, index_col=0)
    is_df.reset_index(inplace=True, drop=True)

    bs_df = pd.read_excel(bs_content, index_col=0)
    bs_df.reset_index(inplace=True, drop=True)

    cf_df = pd.read_excel(cf_content, index_col=0)
    cf_df.reset_index(inplace=True, drop=True)

    invest_df = pd.read_excel(invest_content, index_col=0)
    invest_df.reset_index(inplace=True, drop=True)

    return is_df, bs_df, cf_df, invest_df


# 손익계산서 "날짜+변수"로 변수명 전부 변경
def is_df_preprocessing(IS_data, target):
    IS_data = IS_data[(IS_data['date'].isin(target))]

    # '날짜' 열을 인덱스로, '종목코드' 열을 컬럼으로 사용하여 데이터프레임 재구성
    pivot_is = IS_data.set_index(['date', '종목코드']).unstack('date')

    # 다중 인덱스 컬럼을 단일 인덱스 컬럼으로 조정
    pivot_is.columns = [f'{date} {col}' for col, date in pivot_is.columns]

    # 인덱스 초기화
    pivot_is.reset_index(inplace=True)

    # 한글 종목약명, 시장구분 변수들은 날짜 붙여줄 필요 없음
    pivot_is.drop(['2022/09 한글 종목약명', '2022/12 한글 종목약명', '2023/03 한글 종목약명', '2022/09 시장구분', '2022/12 시장구분','2023/03 시장구분'], axis=1, inplace=True)

    # 선택한 열의 변수 이름을 변경
    pivot_is = pivot_is.rename(columns={'2022/06 한글 종목약명': '한글 종목약명', '2022/06 시장구분': '시장구분'})

    return pivot_is


# 재무상태표 "날짜+변수"로 변수명 전부 변경
def bs_df_preprocessing(BS_data, target):
    # target에 해당하는 날짜만 담기
    BS_data = BS_data[(BS_data['날짜'].isin(target))]

    # 변경된 '날짜' 열을 다시 원하는 형식으로 변환
    BS_data['날짜'] = BS_data['날짜'].dt.strftime('%Y/%m')

    # 'date' 열을 인덱스로, '종목코드' 열을 컬럼으로 사용하여 데이터프레임 재구성
    pivot_bs = BS_data.set_index(['날짜', '종목코드']).unstack('날짜')

    # 다중 인덱스 컬럼을 단일 인덱스 컬럼으로 조정
    pivot_bs.columns = [f'{date} {col}' for col, date in pivot_bs.columns]

    # 인덱스 초기화
    pivot_bs.reset_index(inplace=True)

    # 한글 종목약명, 시장구분 변수들은 날짜 붙여줄 필요 없음
    pivot_bs.drop(['2022/09 한글 종목약명', '2022/12 한글 종목약명', '2023/03 한글 종목약명', '2022/09 시장구분', '2022/12 시장구분','2023/03 시장구분'], axis=1, inplace=True)

    # 선택한 열의 변수 이름을 변경
    pivot_bs = pivot_bs.rename(columns={'2022/06 한글 종목약명': '한글 종목약명', '2022/06 시장구분': '시장구분'})

    return pivot_bs


# 현금흐름표 "날짜+변수" 변수명 생성
def cf_df_preprocessing(CF_data, target):
    # target에 해당하는 날짜만 담게
    CF_data = CF_data[(CF_data['날짜'].isin(target))]

    # 'date' 열을 인덱스로, '종목코드' 열을 컬럼으로 사용하여 데이터프레임 재구성
    pivot_cf = CF_data.set_index(['날짜', '종목코드']).unstack('날짜')

    # 다중 인덱스 컬럼을 단일 인덱스 컬럼으로 조정
    pivot_cf.columns = [f'{date} {col}' for col, date in pivot_cf.columns]

    # 인덱스 초기화
    pivot_cf.reset_index(inplace=True)

    # 한글 종목약명, 시장구분 변수들은 날짜 붙여줄 필요 없음
    pivot_cf.drop(['2022/09 한글 종목약명', '2022/12 한글 종목약명', '2023/03 한글 종목약명', '2022/09 시장구분', '2022/12 시장구분','2023/03 시장구분'], axis=1, inplace=True)

    # 선택한 열의 변수 이름을 변경
    pivot_cf = pivot_cf.rename(columns={'2022/06 한글 종목약명': '한글 종목약명', '2022/06 시장구분': '시장구분'})

    return pivot_cf


# 투자지표 "날짜+변수" 변수명 생성
def invest_df_preprocessing(invest_data, target2):
    # target2에 해당하는 날짜만 담기
    invest_data = invest_data[(invest_data['date'].isin(target2))]

    # 'date' 열을 다시 원하는 형식으로 변환
    invest_data['date'] = pd.to_datetime(invest_data['date'], format='%b-%y')
    invest_data['date'] = invest_data['date'].dt.strftime('%Y/%m')

    # 'date' 열을 인덱스로, '종목코드' 열을 컬럼으로 사용하여 데이터프레임 재구성
    pivot_vm = invest_data.set_index(['date', '종목코드']).unstack('date')

    # 다중 인덱스 컬럼을 단일 인덱스 컬럼으로 조정
    pivot_vm.columns = [f'{date} {col}' for col, date in pivot_vm.columns]

    # 인덱스 초기화
    pivot_vm.reset_index(inplace=True)

    # 한글 종목약명, 시장구분 변수들은 날짜 붙여줄 필요 없음(위 3개 재무제표와 날짜 다름 유의)
    pivot_vm.drop(['2020/12 한글 종목약명', '2021/12 한글 종목약명', '2022/12 한글 종목약명', '2020/12 시장구분', '2021/12 시장구분','2022/12 시장구분'], axis=1, inplace=True)

    # 선택한 열의 변수 이름을 변경
    pivot_vm = pivot_vm.rename(columns={'2019/12 한글 종목약명': '한글 종목약명', '2019/12 시장구분': '시장구분'})

    return pivot_vm


#변수 생성한 데이터 병합
def merge_kr_financial_df(data_is, data_bs, data_cf, data_vm):
    # 열 이름 변경
    data_is = data_is.rename(columns={'한글 종목약명': '기업'})
    data_bs = data_bs.rename(columns={'한글 종목약명': '기업'})
    data_cf = data_cf.rename(columns={'한글 종목약명': '기업'})
    data_vm = data_vm.rename(columns={'한글 종목약명': '기업'})

    # 열 제거
    data_is = data_is.drop(columns=['종목코드', '시장구분'])
    data_bs = data_bs.drop(columns=['종목코드', '시장구분'])
    data_cf = data_cf.drop(columns=['종목코드', '시장구분'])
    data_vm = data_vm.drop(columns=['종목코드', '시장구분'])

    # 국내 기업 리스트
    comp_df,_ = get_comp_list()
    fin = comp_df

    # 중복 행 제거
    unique_fin = fin.drop_duplicates(subset='기업')

    # 손익계산서 중복 행 제거
    data_is = data_is.drop_duplicates(subset='기업')

    # 국내 기업들 중 ex.손익계산서 데이터가 없는 기업 확인
    unique_fin_companies = unique_fin[~unique_fin['기업'].isin(data_is['기업'])]
    unique_data_is_companies = data_is[~data_is['기업'].isin(unique_fin['기업'])]

    # 국내 기업 리스트에 손익계산서 merge
    merged_is = pd.merge(unique_fin, data_is, on='기업', how='left')

    # 재무상태표 merge
    merged_bs = pd.merge(merged_is, data_bs, on='기업', how='left')

    # 현금흐름표 merge
    merged_cf = pd.merge(merged_bs, data_cf, on='기업', how='left')

    # 투자지표 merge
    merged_vm = pd.merge(merged_cf, data_vm, on='기업', how='left')

    return merged_vm


# 국내 (억) 단위 생략, 환율 처리
def apply_exchange_rate(df, columns_to_multiply, exchange_rate):
    for column in columns_to_multiply:
        df[column] = df[column] * exchange_rate * 100000000
    return df


# 환율 * 억(원) 곱해주는 함수
#ROE 파생변수 생성
def roe_calculation(merged_vm, target_date):
    # ROE = 당기순이익 / 자본 X 100
    merged_vm[str(target_date[0])+' ROE'] = merged_vm[str(target_date[0])+' 당기순이익'] / merged_vm[str(target_date[0])+' 자본'] * 100
    merged_vm[str(target_date[1])+' ROE'] = merged_vm[str(target_date[1])+' 당기순이익'] / merged_vm[str(target_date[1])+' 자본'] * 100
    merged_vm[str(target_date[2])+' ROE'] = merged_vm[str(target_date[2])+' 당기순이익'] / merged_vm[str(target_date[2])+' 자본'] * 100
    merged_vm[str(target_date[3])+' ROE'] = merged_vm[str(target_date[3])+' 당기순이익'] / merged_vm[str(target_date[3])+' 자본'] * 100



    # 환율 곱해줘야 할 변수들
    column_names = ['매출액', '매출원가', '매출총이익', '판매비와관리비', '영업이익', '당기순이익','자산', '부채', '자본', '영업활동', '투자활동', '재무활동']

    # 환율 곱해줘야 할 변수들 (데이터프레임 내에서)
    columns_to_multiply = [col for col in merged_vm.columns if any(name in col for name in column_names)]

    # 09-09 04:26 기준 환율: 0.00075
    exchange_rate = 0.00075
    kr_fin = apply_exchange_rate(merged_vm, columns_to_multiply, exchange_rate)

    return kr_fin


#main 실행 함수
'''
#fin_date: 최근 재무제표를 불러온 날짜 입력 (%Y%m%d)
#target_date: 불러올 날짜 리스트 입력 ex)['2022/06', '2022/09', '2022/12', '2023/03']
#target_date2: 투자지표 불러올 날짜 리스트 입력 ex)['Dec-19', 'Dec-20', 'Dec-21', 'Dec-22']
'''
def financial_sim_main(fin_date, target_date, target_date2):
    s3, bucket_name, _ = s3_setting()
    is_df, bs_df, cf_df, invest_df = kr_financial_data_for_sim(fin_date, s3, bucket_name)

    pivot_is = is_df_preprocessing(is_df, target_date)
    pivot_bs = bs_df_preprocessing(bs_df, target_date)
    pivot_cf = cf_df_preprocessing(cf_df, target_date)
    pivot_vm = invest_df_preprocessing(invest_df, target_date2)

    kr_financial_df_merged = merge_kr_financial_df(pivot_is, pivot_bs, pivot_cf, pivot_vm)

    kr_fin = roe_calculation(kr_financial_df_merged, target_date)
    
    return kr_fin


#최종 데이터셋 취합 위해 기본데이터셋 불러오기
def sim_main_data(s3, bucket_name):

    object_name = '기업리스트/국내_유사도_기본.csv'

    # S3에서 파일 내용을 읽어옴
    response = s3.get_object(Bucket=bucket_name, Key=object_name)
    content = response['Body'].read()

    # 읽어온 엑셀 파일을 데이터프레임으로 변환
    df = pd.read_csv(content, encoding='cp949')
    df.reset_index(inplace=True, drop=True)

    return df
