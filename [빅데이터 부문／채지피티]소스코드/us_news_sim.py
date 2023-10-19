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



#미국 뉴스 감성분석 데이터 클라우드에서 불러오기(취합)
def us_news_data_for_sim(s3, bucket_name):

    pickle_filename = 'usa_news_pipe.pickle'  # 파일 이름
    object_nm = str(datetime.datetime.now().strftime('%Y%m%d')) + '/usa_news_crawling/' # 폴더 이름
    object_name = object_nm + str(pickle_filename)

    # S3에서 파일 내용을 읽어옴
    response = s3.get_object(Bucket=bucket_name, Key=object_name)
    content = response['Body'].read()

    # 읽어온 피클 파일을 데이터프레임으로 변환
    df = pd.read_pickle(content)
    df.reset_index(inplace=True, drop=True)

    #어제의 날짜 데이터 불러오기
    yesterday = datetime.datetime.now() - timedelta(days=1)
    formatted_yesterday = yesterday.strftime('%Y%m%d')
    object_nm2 = str(formatted_yesterday) + '/usa_news_crawling/' # 폴더 이름
    object_name2 = object_nm2 + str(pickle_filename)

    # S3에서 파일 내용을 읽어옴
    response2 = s3.get_object(Bucket=bucket_name, Key=object_name2)
    content2 = response2['Body'].read()

    # 읽어온 피클 파일을 데이터프레임으로 변환
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


#감성지수, 감성비율 계산하여 추가
def us_sentiment_for_sim(usa_news):

    usa_news['label'] = usa_news['감성분석'].apply(lambda x: x[0]['label'])
    usa_news['score'] = usa_news['감성분석'].apply(lambda x: x[0]['score'])
    usa_news.drop(columns=['감성분석'], inplace=True)
    usa_news = usa_news[['Ticker', 'label', 'score']]


    # [Ticker, label]로 그룹화하고 각 그룹의 개수, 비율, 그리고 'score' 열의 평균 계산
    df_grouped = usa_news.groupby(['Ticker', 'label']).agg({
        'score': ['count', 'mean']
    }).reset_index()

    # 열 이름 재설정
    df_grouped.columns = ['Ticker', 'label', 'count', 'score_mean']

    # 각 그룹에서 전체 개수를 기준으로 비율 계산
    total_counts = df_grouped.groupby('Ticker')['count'].transform('sum')
    df_grouped['ratio'] = df_grouped['count'] / total_counts

    # Pivot을 사용하여 각 Ticker 별로 레이블에 대한 열을 추가
    df_pivoted = df_grouped.pivot(index='Ticker', columns='label', values=['count', 'score_mean', 'ratio'])

    # 열 이름 재설정
    df_pivoted.columns = [f'{col[0]}_{col[1]}' for col in df_pivoted.columns]

    # 결측치(NaN)를 0으로 채우기
    df_pivoted = df_pivoted.fillna(0)
    df_pivoted = df_pivoted.reset_index()

    # 'count' 열을 추출하여 열별로 총합 계산
    total_counts = df_pivoted[['count_negative', 'count_neutral', 'count_positive']].sum().sum()

    # 열 이름 수정
    new_columns = ['count_negative_ratio', 'count_neutral_ratio', 'count_positive_ratio']
    # 각 열의 값을 비율로 변경
    df_pivoted[new_columns] = df_pivoted[['count_negative', 'count_neutral', 'count_positive']] / total_counts*1000

    df = df_pivoted
    # 'count_negative_ratio' 열을 'score_mean_negative' 및 'ratio_negative' 값에 곱하기
    df['score_mean_negative'] = df['score_mean_negative'] * df['count_negative_ratio']
    df['ratio_negative'] = df['ratio_negative'] * df['count_negative_ratio']

    # 'count_positive_ratio' 열을 'score_mean_positive' 및 'ratio_positive' 값에 곱하기
    df['score_mean_positive'] = df['score_mean_positive'] * df['count_positive_ratio']
    df['ratio_positive'] = df['ratio_positive'] * df['count_positive_ratio']

    # 'count_positive_ratio' 열을 'score_mean_positive' 및 'ratio_positive' 값에 곱하기
    df['score_mean_neutral'] = df['score_mean_neutral'] * df['count_neutral_ratio']
    df['ratio_neutral'] = df['ratio_neutral'] * df['count_neutral_ratio']

    # 'count_negative_ratio'와 'count_positive_ratio' 열 삭제
    df = df.drop(['count_negative_ratio', 'count_positive_ratio','count_neutral_ratio'], axis=1)

    # 비율 열을 계산하여 추가
    df['new_ratio_negative'] = df['ratio_negative'] / (df['ratio_negative'] + df['ratio_positive']+df['ratio_neutral'])
    df['new_ratio_positive'] = df['ratio_positive'] / (df['ratio_negative'] + df['ratio_positive']+df['ratio_neutral'])
    df['new_ratio_neutral'] = df['ratio_neutral'] / (df['ratio_negative'] + df['ratio_positive']+df['ratio_neutral'])

    return df


#main 실행함수
def us_sentiment_sim_main():
    s3, bucket_name, _ = s3_setting()
    merged_df = us_news_data_for_sim(s3, bucket_name)
    df = us_sentiment_for_sim(merged_df)

    return df
