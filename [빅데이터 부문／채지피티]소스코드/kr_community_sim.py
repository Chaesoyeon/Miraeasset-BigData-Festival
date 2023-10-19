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


#화제성 계산 위해 네이버 종목토론방 데이터 클라우드에서 불러오기(취합)
def sim_krcommu_data():
    s3, bucket_name, _ = s3_setting()
    pickle_filename = '네이버_종목토론방_sentimnt.pickle'  # 파일 이름
    object_nm = str(datetime.datetime.now().strftime('%Y%m%d')) + '/kr_community_crawling/' # 폴더 이름
    object_name = object_nm + str(pickle_filename)

    # S3에서 파일 내용을 읽어옴
    response = s3.get_object(Bucket=bucket_name, Key=object_name)
    content = response['Body'].read()

    # 읽어온 엑셀 파일을 데이터프레임으로 변환
    df = pd.read_pickle(content)
    df.reset_index(inplace=True, drop=True)


    #어제의 날짜 데이터 불러오기
    yesterday = datetime.datetime.now() - timedelta(days=1)
    formatted_yesterday = yesterday.strftime('%Y%m%d')
    object_nm2 = str(formatted_yesterday) + '/kr_community_crawling/' # 폴더 이름
    object_name2 = object_nm2 + str(pickle_filename)

    # S3에서 파일 내용을 읽어옴
    response2 = s3.get_object(Bucket=bucket_name, Key=object_name2)
    content2 = response2['Body'].read()

    # 읽어온 엑셀 파일을 데이터프레임으로 변환
    df2 = pd.read_pickle(content2)
    df2.reset_index(inplace=True, drop=True)

    merged_df = pd.concat([df, df2], ignore_index=True)

    # '날짜' 열을 날짜 형식으로 변환
    merged_df['날짜'] = pd.to_datetime(merged_df['날짜'], errors='coerce')

    # 가장 오래된 날짜 찾기
    oldest_date = merged_df['날짜'].min()

    # 가장 오래된 날짜에 해당하는 행 제거
    merged_df = merged_df[merged_df['날짜'] != oldest_date]
 
    return merged_df


#날짜가 가까울수록 가중치 주기
def kr_commu_weight(df, date_column):
    df[date_column] = pd.to_datetime(df[date_column], format = '%Y-%m-%d')
    
    today = datetime.datetime.today()
    df['화제성'] = (today - df[date_column]).dt.days + 1
    df['화제성'] = df['화제성'].max() - df['화제성'] + 1
    
    return df


# 3일 동안의 화제성 계산
def kr_commu_topicality(df, date_column):
    df[date_column] = pd.to_datetime(df[date_column])
    
    recent_date = df[date_column].max()
    start_date = recent_date - pd.DateOffset(days = 2)
    
    df = df[(df[date_column] >= start_date) & (df[date_column] <= recent_date)]
    
    topicality = df.groupby(date_column)['화제성'].sum().reset_index()
    
    # 평균 가중치 계산
    mean_topicality = round(topicality['화제성'].mean(), 1)
    
    return mean_topicality

#main 실행 함수
def kr_community_sim_main():
    merged_data = sim_krcommu_data()

    weight_df = kr_commu_weight(merged_data, '날짜')

    # '기업' 열 목록 추출
    names = weight_df['기업'].unique()

    # 빈 데이터프레임 생성
    result_df = pd.DataFrame(columns=['기업', '화제성'])

    for name in names:
        name_df = weight_df[weight_df['기업'] == name]
        topicality_value = kr_commu_topicality(name_df, '날짜')
        
        # 새로운 행 추가
        new_row = pd.DataFrame({'기업': [name], '화제성': [topicality_value]})
        result_df = pd.concat([result_df, new_row], ignore_index=True)
    
    return result_df

