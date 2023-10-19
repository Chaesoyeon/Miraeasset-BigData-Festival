import requests
from bs4 import BeautifulSoup
import os
import datetime as dt
import pandas as pd
from datetime import timedelta
import re
from io import BytesIO
from s3_setting import *


#재무제표 데이터 클라우드에서 불러오기
def us_financial_data_for_sim(fin_date, s3, bucket_name):

    is_filename = 'IS_재무제표.xlsx'
    bs_filename = 'BS_재무제표.xlsx'
    cf_filename = 'CF_재무제표.xlsx'
    VM_filename = 'VM_재무제표.xlsx'
    all_filename = 'all_data_재무제표.xlsx'

    is_object_name = str(fin_date) + '/financial_us/' + str(is_filename)
    bs_object_name = str(fin_date) + '/financial_us/' + str(bs_filename)
    cf_object_name = str(fin_date) + '/financial_us/' + str(cf_filename)
    vm_object_name = str(fin_date) + '/financial_us/' + str(VM_filename)
    all_object_name = str(fin_date) + '/financial_us/' + str(all_filename)

    # S3에서 파일 내용을 읽어옴
    is_response = s3.get_object(Bucket=bucket_name, Key=is_object_name)
    is_content = is_response['Body'].read()

    bs_response = s3.get_object(Bucket=bucket_name, Key=bs_object_name)
    bs_content = bs_response['Body'].read()

    cf_response = s3.get_object(Bucket=bucket_name, Key=cf_object_name)
    cf_content = cf_response['Body'].read()

    vm_response = s3.get_object(Bucket=bucket_name, Key=vm_object_name)
    vm_content = vm_response['Body'].read()

    all_response = s3.get_object(Bucket=bucket_name, Key=all_object_name)
    all_content = all_response['Body'].read()


    # 읽어온 엑셀 파일을 데이터프레임으로 변환
    is_df = pd.read_excel(is_content, index_col=0)
    is_df.reset_index(inplace=True, drop=True)

    bs_df = pd.read_excel(bs_content, index_col=0)
    bs_df.reset_index(inplace=True, drop=True)

    cf_df = pd.read_excel(cf_content, index_col=0)
    cf_df.reset_index(inplace=True, drop=True)

    vm_df = pd.read_excel(vm_content, index_col=0)
    vm_df.reset_index(inplace=True, drop=True)

    all_df = pd.read_excel(all_content, index_col=0)
    all_df.reset_index(inplace=True, drop=True)

    return is_df, bs_df, cf_df, vm_df, all_df


#기간 맞지 않는 항목 처리(재무제표 전체 항목)
def all_date_newcol(result_all, target):

    # result_all에는 손익계산서, 재무상태표, 현금흐름표에 해당하는 데이터만 담고 있음. 투자지표는 result_vm에서 추가로 불러와야 함
    # 국내 재무제표 기준 재무상태표의 자본, 투자지표의 ROE에 해당하는 항목이 미국 재무제표 데이터에는 없어 파생변수로써 추후 추가 계산 예정
    all_data = result_all[['symbol','asOfDate','periodType',
                        # 손익계산서
                        'TotalRevenue', 'CostOfRevenue', 'GrossProfit',
                        'SellingGeneralAndAdministration', 'OperatingIncome', 'NetIncome',
                        # 재무상태표
                        'TotalAssets', 'TotalLiabilitiesNetMinorityInterest',
                        # 현금흐름표
                        'CashFlowFromContinuingOperatingActivities', 'CashFlowFromContinuingInvestingActivities', 
                        'CashFlowFromContinuingFinancingActivities']]
    # 변수명 한글로 통일
    new_columns_all = {
        'symbol': 'Ticker',
        'TotalRevenue': '매출액',
        'CostOfRevenue': '매출원가',
        'GrossProfit': '매출총이익',
        'SellingGeneralAndAdministration': '판매비와관리비',
        'OperatingIncome': '영업이익', 
        'NetIncome':'당기순이익',
        'TotalAssets':'자산',
        'TotalLiabilitiesNetMinorityInterest': '부채',
        'CashFlowFromContinuingOperatingActivities': '영업활동', 
        'CashFlowFromContinuingInvestingActivities': '투자활동',              
        'CashFlowFromContinuingFinancingActivities': '재무활동'
    }

    all_data.rename(columns=new_columns_all, inplace=True)

    # 자본에 해당하는 변수가 없어 자산 - 부채로 파생변수 생성
    all_data['자본'] = all_data['자산'] - all_data['부채']


    # 조건에 맞는 행들을 선택(기간이 TTM인 것과 정해진 날짜 이외의 것은 제거)
    all_data = all_data[(all_data['periodType'] == '3M') & (all_data['asOfDate'].isin(target))]
    all_data = all_data.reset_index(drop=True)

    # 2022-12-31 값을 2022/12로 통일
    all_data['asOfDate'] = all_data['asOfDate'].str.replace('-31', '/').str.slice(0, 7)
    all_data['asOfDate'] = all_data['asOfDate'].str.replace('-', '/')

    all_data = all_data.drop_duplicates(subset=['asOfDate', 'Ticker'])

    # 'asOfDate' 열을 인덱스로, '종목코드' 열을 컬럼으로 사용하여 데이터프레임 재구성
    pivot_all = all_data.set_index(['asOfDate', 'Ticker']).unstack('asOfDate')

    # 다중 인덱스 컬럼을 단일 인덱스 컬럼으로 조정
    pivot_all.columns = [f'{date} {col}' for col, date in pivot_all.columns]

    # 인덱스 초기화
    pivot_all.reset_index(inplace=True)

    # periodType 변수는 날짜 붙여줄 필요 없음(위 3개 재무제표와 날짜 다름 유의)
    pivot_all.drop(['2022/09 periodType', '2022/12 periodType', '2023/03 periodType'], axis=1, inplace=True)

    # 선택한 열의 변수 이름을 변경
    pivot_all = pivot_all.rename(columns={'2022/06 periodType': 'periodType'})

    return pivot_all


#기간 맞지 않는 항목 처리(VM)
def vm_var_processing(result_vm, target2):

    # 투자지표 항목 중 아래 변수만 담기
    # 국내 재무제표 기준 투자지표로 분류되는 ROE는 추후 반영
    result_vm = result_vm[['symbol', 'asOfDate', 'periodType', 'PeRatio', 'PsRatio', 'PbRatio', 'EnterprisesValueEBITDARatio']]

    # 변수명 한글로 통일
    new_columns_vm = {
        'symbol': 'Ticker',
        'PeRatio': 'PER',
        'PsRatio': 'PSR',
        'PbRatio': 'PBR',
        'EnterprisesValueEBITDARatio': 'EV/EBITDA'
    }

    result_vm.rename(columns=new_columns_vm, inplace=True)


    # 조건에 맞는 행들을 선택(기간이 TTM인 것과 정해진 날짜 이외의 것은 제거)
    result_vm = result_vm[(result_vm['periodType'] == '3M') & (result_vm['asOfDate'].isin(target2))]
    result_vm = result_vm.reset_index(drop=True)

    # 2022-12-31 값을 2022/12로 통일
    result_vm['asOfDate'] = result_vm['asOfDate'].str.replace('-31', '/').str.slice(0, 7)
    result_vm['asOfDate'] = result_vm['asOfDate'].str.replace('-', '/')

    # 'date' 열을 인덱스로, '종목코드' 열을 컬럼으로 사용하여 데이터프레임 재구성
    pivot_vm = result_vm.set_index(['asOfDate', 'Ticker']).unstack('asOfDate')

    # 다중 인덱스 컬럼을 단일 인덱스 컬럼으로 조정
    pivot_vm.columns = [f'{date} {col}' for col, date in pivot_vm.columns]

    # 인덱스 초기화
    pivot_vm.reset_index(inplace=True)

    # 선택한 열의 변수 이름을 변경
    pivot_vm = pivot_vm.rename(columns={'2022/12 periodType': 'periodType'})
    
    return pivot_vm


#재무제표 데이터 병합
def merge_us_financial_df(vm_fin, pivot_all):
    unique_fin = get_uscomp_list()

    pivot_all = pivot_all.drop_duplicates(subset='Ticker')
    vm_fin = vm_fin.drop_duplicates(subset='Ticker')

    # 미국 기업들 중 ex.투자지표 제외 재무제표 데이터가 없는 기업 확인
    unique_fin_companies = unique_fin[~unique_fin['Ticker'].isin(pivot_all['Ticker'])]
    unique_data_is_companies = pivot_all[~pivot_all['Ticker'].isin(unique_fin['Ticker'])]

    # 미국 기업 리스트에 투자지표 제외 전체 재무제표 데이터 담은 pivot_all merge
    merged_all = pd.merge(unique_fin, pivot_all, on='Ticker', how='left')

    # 투자지표 데이터 담은 vm_fin merge
    merged_vm = pd.merge(merged_all, vm_fin, on=['Ticker','periodType'], how='left')

    merged_vm.drop(['periodType'], axis=1, inplace=True)

    return merged_vm


#ROE 계산 추가
def us_roe_calculation(merged_vm, target_date):

    target_date = [date.split('-')[0] + '/' + date.split('-')[1] for date in target_date]

    # ROE = 당기순이익 / 자본 X 100
    merged_vm[str(target_date[0])+' ROE'] = merged_vm[str(target_date[0])+' 당기순이익'] / merged_vm[str(target_date[0])+' 자본'] * 100
    merged_vm[str(target_date[1])+' ROE'] = merged_vm[str(target_date[1])+' 당기순이익'] / merged_vm[str(target_date[1])+' 자본'] * 100
    merged_vm[str(target_date[2])+' ROE'] = merged_vm[str(target_date[2])+' 당기순이익'] / merged_vm[str(target_date[2])+' 자본'] * 100
    merged_vm[str(target_date[3])+' ROE'] = merged_vm[str(target_date[3])+' 당기순이익'] / merged_vm[str(target_date[3])+' 자본'] * 100



    # 환율 곱해줘야 할 변수들
    column_names = ['매출액', '매출원가', '매출총이익', '판매비와관리비', '영업이익', '당기순이익','자산', '부채', '자본', '영업활동', '투자활동', '재무활동']

    # 아래의 변수들은 All numbers in thousands로 단위가 축소되어 있어 1000씩 곱해줌
    columns_to_multiply_by_1000 = [col for col in merged_vm.columns if any(name in col for name in column_names)]

    merged_vm[columns_to_multiply_by_1000] *= 1000

    return merged_vm



#fin_date: 최근 재무제표를 불러온 날짜 입력 (%Y%m%d)
#target: all data 불러올 날짜 리스트 입력 ex)['2022-06-30', '2022-09-30', '2022-12-31', '2023-03-31']
#target2: 투자지표 불러올 날짜 리스트 입력 ex)['2019-12-31', '2020-12-31', '2021-12-31', '2022-12-31']
                # 국내 투자지표와 동일하게 기간 설정(나머지 재무제표 종류들의 날짜와 다름)
def us_financial_sim_main(fin_date, target, target2):
    s3, bucket_name, _ = s3_setting()
    is_df, bs_df, cf_df, vm_df, all_df = us_financial_data_for_sim(fin_date, s3, bucket_name)

    pivot_all = all_date_newcol(all_df, target)
    pivot_vm = vm_var_processing(vm_df, target2)

    merged_vm = merge_us_financial_df(pivot_vm, pivot_all)

    merged_vm = us_roe_calculation(merged_vm, target)

    return merged_vm



def us_sim_main_data(s3, bucket_name):

    object_name = '기업리스트/해외_유사도_기본.csv'

    # S3에서 파일 내용을 읽어옴
    response = s3.get_object(Bucket=bucket_name, Key=object_name)
    content = response['Body'].read()

    # 읽어온 엑셀 파일을 데이터프레임으로 변환
    df = pd.read_csv(content, encoding='cp949')
    df.reset_index(inplace=True, drop=True)

    return df
