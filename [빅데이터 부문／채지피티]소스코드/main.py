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
import pandas as pd
import datetime
import io
from collections import Counter
from konlpy.tag import Komoran

#s3 세팅 -  오브젝트 스토리지 연결
from s3_setting import *

#데이터 크롤링
from us_financial import *
from kr_financial import *
from stocktwits_crawling import *
from daum_news_crawling import *
from usa_news_crawling import *
from kr_stock_crawling import *
from kr_community_crawling import *
from usa_stock_crawling import *

#크롤링 데이터 처리
from stocktwits_sentiment import *
from naver_toron_sentiment import *
from usa_news_pipe import *
from daum_news_pipe import *

#시각화 데이터 처리
from us_chart import *
from kr_chart import *
from us_keyword_chart import *
from kr_keyword_chart import *
from change_average_us import *
from change_average_kr import *

#유사도용 최종 데이터셋
from financial_sim import *
from us_financial_sim import *
from kr_news_sim import *
from us_news_sim import *
from us_community_sim import *
from kr_community_sim import *

#유사도 계산
from gower_similarity import *



###########
# 크롤링 #
##########

'''하루치 크롤링 데이터 스토리지에 저장: %Y%m%d/각각의 폴더에 저장'''
def crawling_main():

    #클라우드 지정
    s3, bucket_name, _ = s3_setting()
    
    #국내기업 리스트 불러오기
    comp_df,_ = get_comp_list()
    kr_company_list = comp_df.기업
    #해외기업 리스트 불러오기
    us_company_list = get_uscomp_list()

    #크롤링
    kr_community_crawling_func() #국내 종목토론방
    get_stock_kr_main() #국내 주가 데이터
    usa_stock_crawling()    #해외 주가 데이터
    
    # 국내 뉴스 크롤링 데이터 저장할 폴더 생성
    daum_object_nm = str(datetime.datetime.now().strftime('%Y%m%d')) + '/daum_news_crawling/'
    s3.put_object(Bucket=bucket_name, Key=daum_object_nm)
    
    for kr_company in kr_company_list:
        daum_news_crawling(kr_company) #국내 뉴스
        
        

    # 미국 뉴스 크롤링 데이터 저장할 폴더 생성
    object_nm_news = str(datetime.datetime.now().strftime('%Y%m%d')) + '/usa_news_crawling/'
    s3.put_object(Bucket=bucket_name, Key=object_nm_news)
    # 미국 종목토론방 크롤링 데이터 저장할 폴더 생성
    object_nm_stocktwits = str(datetime.datetime.now().strftime('%Y%m%d')) + '/stocktwits_crawling/'
    s3.put_object(Bucket=bucket_name, Key=object_nm_stocktwits)
        
    for index, row in us_company_list.iterrows():
        company_name = row['company2']
        ticker = row['Ticker']

        usa_news_crawling(company_name) #해외 뉴스
        stocktwits_crawling(ticker) #해외 종목토론방
    
    
# 재무제표는 3개월마다 실행하여 업데이트
def financial_main():
    get_financial_data()
    get_us_financial_data()

    
    
##############################
#기사/종목토론방 자연어 처리 #
##############################

#크롤링한 데이터 모두 concat
#국/내외 뉴스, 종목토론방 요약 및 감성분석 후 스토리지에 저장

def sum_sentiment_main():

    '''
    kr_sum: 국내뉴스 요약
    kr_sentiment: 국내뉴스 감성, 국내종목토론방 감성분석
    us_summa: 미국뉴스 요약
    us_sentiment: 미국뉴스 감성분석
    stocktwits_sentiment: 미국종목토론방 감성분석
    us_keyword: 미국뉴스 키워드 추출
    kr_keyword: 국내뉴스 키워드 추출
    '''
    
    #필요한 모델 로드
    kr_sum = pipeline("text2text-generation", model="lcw99/t5-large-korean-text-summary", device = 0) 
    kr_sentiment = pipeline("text-classification", model="snunlp/KR-FinBert-SC", device = 0) 

    us_summa = pipeline("summarization", model="human-centered-summarization/financial-summarization-pegasus", device = 0) 
    us_sentiment = pipeline("text-classification", model="ahmedrachid/FinancialBERT-Sentiment-Analysis", device = 0) 

    stocktwits_sentiment = pipeline("text-classification", model="cardiffnlp/twitter-roberta-base-sentiment-latest", device = 0) 
    
    us_keyword = pipeline("token-classification", model="ml6team/keyphrase-extraction-kbir-kpcrowd", device= 0)
    kr_keyword = Komoran() 
    

    
    #모델 파이프라인 실행
    '''
    daum_news_pipe: 국내 뉴스 요약, 감성분석, 키워드 추출: %Y%m%d/daum_news_crawling/daum_news_pipe.pickle
    usa_news_pipe: 해외 뉴스 요약, 감성분석, 키워드 추출: %Y%m%d/usa_news_crawling/usa_news_pipe.pickle
    kr_community_sentiment: 국내 종목토론방 감성분석: %Y%m%d/kr_community_crawling/네이버_종목토론방_sentiment.pickle
    stocktwit_sentiment: 해외 종목토론방 감성분석: %Y%m%d/stocktwits_crawling/stock twits_sentiment.pickle
    '''
    
    daum_news_pipe(kr_sum, kr_sentiment, kr_keyword) 
    usa_news_pipe(us_summa, us_sentiment, us_keyword)
    kr_community_sentiment(kr_sentiment)
    stocktwit_sentiment(stocktwits_sentiment) 
    
    

######################################
#   시각화용 데이터 처리 main 함수   #
######################################

def eda_main():
    uschart_main()
    keyword_chart_us_main()
    
    krchart_main()
    keyword_chart_kr_main()
    
    usavg_main()
    kravg_main()


######################################
# 유사도용 최종 데이터셋 만들기 함수 #
######################################



def gower_sim_main():
    
    crawling_main()
    sum_sentiment_main()
    eda_main()
    
    
 
    '''
    fin_date: 최근 재무제표를 불러온 날짜 입력 (%Y%m%d)

    target_date: 불러올 날짜 리스트 입력 ex)['2022/06', '2022/09', '2022/12', '2023/03']
    target_date2: 투자지표 불러올 날짜 리스트 입력 ex)['Dec-19', 'Dec-20', 'Dec-21', 'Dec-22']

    target: all data 불러올 날짜 리스트 입력 ex)['2022-06-30', '2022-09-30', '2022-12-31', '2023-03-31']
    target2: 투자지표 불러올 날짜 리스트 입력 ex)['2019-12-31', '2020-12-31', '2021-12-31', '2022-12-31']
                    # 국내 투자지표와 동일하게 기간 설정(나머지 재무제표 종류들의 날짜와 다름)
    '''    

    similarity_merge(fin_date='20230831', 
               target_date = ['2022/06', '2022/09', '2022/12', '2023/03'], 
               target_date2= ['Dec-19', 'Dec-20', 'Dec-21', 'Dec-22'], 
               target = ['2022-06-30', '2022-09-30', '2022-12-31', '2023-03-31'], 
               target2 = ['2019-12-31', '2020-12-31', '2021-12-31', '2022-12-31'])
    
    
    

######################################
#              main 함수             #
######################################
    

if __name__ == "__main__":
    gower_sim_main()
    
    
    
    