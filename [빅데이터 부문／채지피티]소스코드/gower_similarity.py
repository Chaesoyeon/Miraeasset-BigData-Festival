import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
from datetime import timedelta
import re
from io import BytesIO
import pickle
import datetime
from s3_setting import *

#유사도용 최종 데이터셋
from financial_sim import *
from us_financial_sim import *
from kr_news_sim import *
from us_news_sim import *
from us_community_sim import *
from kr_community_sim import *


    
def similarity_merge(fin_date, target_date, target_date2, target, target2):
   
    '''
    fin_date: 최근 재무제표를 불러온 날짜 입력 (%Y%m%d)

    target_date: 불러올 날짜 리스트 입력 ex)['2022/06', '2022/09', '2022/12', '2023/03']
    target_date2: 투자지표 불러올 날짜 리스트 입력 ex)['Dec-19', 'Dec-20', 'Dec-21', 'Dec-22']

    target: all data 불러올 날짜 리스트 입력 ex)['2022-06-30', '2022-09-30', '2022-12-31', '2023-03-31']
    target2: 투자지표 불러올 날짜 리스트 입력 ex)['2019-12-31', '2020-12-31', '2021-12-31', '2022-12-31']
                    # 국내 투자지표와 동일하게 기간 설정(나머지 재무제표 종류들의 날짜와 다름)
    '''    
    s3, bucket_name, _ = s3_setting()

    ##국내 유사도 데이터셋 취합
    sim_main_df = sim_main_data(s3, bucket_name)
    kr_fin = financial_sim_main(fin_date, target_date, target_date2)
    #섹터 + 재무제표
    kr_fin_sim = pd.merge(sim_main_df, kr_fin, on='기업', how='left')

    kr_news =  kr_sentiment_sim_main()
    #섹터 + 재무제표 + 뉴스 감성분석
    kr_fin_news_sim = pd.merge(kr_fin_sim, kr_news, on='기업', how='left')

    kr_community = kr_community_sim_main()
    #섹터 + 재무제표 + 뉴스 감성분석 + 화제성
    kr_similarity = pd.merge(kr_fin_news_sim, kr_community, on='기업', how='left')
    
    
    ##미국 유사도 데이터셋 취합
    us_sim_main_df = us_sim_main_data(s3, bucket_name)
    us_fin = us_financial_sim_main(fin_date, target, target2)
    #섹터 + 재무제표
    us_fin_sim = pd.merge(us_sim_main_df, us_fin, on='Ticker', how='left')

    us_news =  us_sentiment_sim_main()
    #섹터 + 재무제표 + 뉴스 감성분석
    us_fin_news_sim = pd.merge(us_fin_sim, us_news, on='Ticker', how='left')

    us_community = us_community_sim_main()
    #섹터 + 재무제표 + 뉴스 감성분석 + 화제성
    us_similarity = pd.merge(us_fin_news_sim, us_community, on='Ticker', how='left')


    #합친 데이터 전처리
    df1, df2 = sim_preprocessing(kr_similarity, us_similarity)


    #클라우드에 업로드 시작
    s3, bucket_name, _ = s3_setting()
    #국내 유사도 데이터셋
    object_nm = str(datetime.datetime.now().strftime('%Y%m%d')) + '/' # 폴더 이름
    csv_filename = '국내_유사도_최종_데이터셋.csv'  # 파일 이름
    csv_buffer = io.BytesIO()
    df1.to_csv(csv_buffer, encoding='cp949', index=False)  # 피클 파일로 저장
    csv_buffer.seek(0)

    object_name = object_nm + str(csv_filename)
    s3.upload_fileobj(csv_buffer, bucket_name, object_name)  # 클라우드에 업로드
    print(f'Uploaded {csv_filename} to S3')


    #미국 유사도 데이터셋
    object_nm2 = str(datetime.datetime.now().strftime('%Y%m%d')) + '/' # 폴더 이름
    csv_filename2 = '해외_유사도_최종_데이터셋.csv'  # 파일 이름
    csv_buffer2 = io.BytesIO()
    df2.to_csv(csv_buffer2, encoding='cp949', index=False) # csv 파일로 저장
    csv_buffer2.seek(0)

    object_name2 = object_nm2 + str(csv_filename2)
    s3.upload_fileobj(csv_buffer2, bucket_name, object_name2)  # 클라우드에 업로드
    print(f'Uploaded {csv_filename2} to S3')



#Nan, 데이터 type 처리
def sim_preprocessing(df1, df2):

    # 모든 NaN 값을 0으로 바꾸기
    df1.fillna(0, inplace=True)
    df2.fillna(0, inplace=True)
    df1.set_index('기업', inplace=True)
    df2.set_index('Ticker', inplace=True)

    string_columns = df2.select_dtypes(include=['object'])
    for col in string_columns.columns:
        try:
            df2[col] = df2[col].astype(float)
        except ValueError:
            df2[col] = 0.0  # 변환이 불가능한 경우 0으로 처리


    return df1, df2

#항목 필터링에 따라 가중치 변경
def make_weights(df2, filter_list, w):

    선택 = filter_list
    가중치 = w

    weights = {col: 1 for col in df2.columns}

    if '대분류_종합' in 선택:
        weights = weights
    if '섹터' in 선택:
        for col in df2.columns[:147]:
            weights[col] = 가중치
    if '재무제표_종합' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col)]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if '매출액' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and '매출액' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if '매출원가' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and '매출원가' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if '매출총이익' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and '매출총이익' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if '판매비와관리비' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and '판매비와관리비' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if '영업이익' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and '영업이익' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if '당기순이익' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and '당기순이익' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if '자산' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and '자산' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if '부채' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and '부채' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if '자본' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and '자본' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if '영업활동' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and '영업활동' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if '투자활동' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and '투자활동' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if '재무활동' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and '재무활동' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if 'PER' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and 'PER' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if 'PSR' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and 'PSR' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if 'PBR' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and 'PBR' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if 'EV/EBITDA' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and 'EV/EBITDA' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if 'ROE' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if re.match(r'^\d{4}/\d{2}', col) and 'ROE' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if '뉴스_종합' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if any(keyword in col for keyword in ['negative', 'neutral', 'positive'])]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if '감정지수' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if 'score_mean' in col]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if '감정비율' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if any(keyword in col for keyword in ['ratio_negative', 'ratio_neutral', 'ratio_positive'])]
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    if '화제성' in 선택:
        matching_indexes = [idx for idx, col in enumerate(df2.columns) if col == '화제성']
        for col in df2.columns[matching_indexes]:
            weights[col] = 가중치
    return weights


# Gower 거리 계산 함수 정의
def gower_distance(x, y, weight=None):
    distance_sum = 0
    weight_sum = 0

    for col in x.index:
        if x[col].dtype == 'float64':
            distance = abs(x[col] - y[col])  # 연속형 변수인 경우 거리 계산
        else:
            distance = 0 if x[col] == y[col] else 1  # 범주형 변수인 경우 거리 계산

        if weight is not None:
            weight_i = weight[col]
        else:
            weight_i = 1

        distance_sum += weight_i * distance
        weight_sum += weight_i


    if weight_sum == 0:
        return 0  # 가중치 합이 0인 경우 예외 처리

    return distance_sum / weight_sum



# Gower 거리 계산
def gower_result(target_row, df2, weights):
    gower_dist = gower_distance(target_row.iloc[0], df2, weight=weights)
    gower_dist.sort_values(ascending=True, inplace=True)
    gower_dist = pd.DataFrame(gower_dist[:3])
    gower_dist.rename(columns={'알루미늄': 'Gower'}, inplace=True)
    return gower_dist

def make_us_recommendation(target_name, filtering_list, weights, df1, df2):
    target_row = df1.loc[target_name].to_frame().T
    wt = make_weights(df2, filtering_list, weights)
    result = gower_result(target_row, df2, wt)
    return result

def make_kr_recommendation(target_name, filtering_list, weights, df1, df2):
    target_row = df2.loc[target_name].to_frame().T
    wt = make_weights(df1, filtering_list, weights)
    result = gower_result(target_row, df1, wt)
    return result

