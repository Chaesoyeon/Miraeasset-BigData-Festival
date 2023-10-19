from datetime import datetime, timedelta
import io
import boto3
import selenium
from selenium import webdriver
import time
import bs4
import requests
import urllib.request
import json
import os
import numpy as np
import re
import random
import chromedriver_autoinstaller
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.remote.webelement import WebElement, By
from webdriver_manager.chrome import ChromeDriverManager
from ast import literal_eval
from urllib import parse
import pandas as pd
from s3_setting import *


#데이터프레임으로 저장하고 클라우드에 올리는 함수
def make_df_result(info_, column_):

    column_name = []
    content_arrange = []
    press_name = ''
    searchword = ''
    for key, value in enumerate(info_):
        press_name = value['press_name']
        searchword = value['searchword']

        temp_arrange = []
        for key_l, vlaue_l in enumerate(column_):
            if vlaue_l in value:
                temp_arrange.append(value[vlaue_l])
            else:
                temp_arrange.append('')

            column_name.append(column_[vlaue_l])

        content_arrange.append(temp_arrange)

    column_name = column_name[0:len(column_)]
    now = datetime.now()
    list_pandas = pd.DataFrame.from_records(content_arrange)
    list_pandas.columns = ["언론사", "검색어", "기사종류", "제목", "날짜", "기사내용", "링크"]
    
    #클라우드에 업로드 시작
    s3, bucket_name, _ = s3_setting()
    
    # 폴더 지정
    object_nm = str(datetime.now().strftime('%Y%m%d')) + '/usa_news_crawling/'
    
    now = datetime.now()
    
    pickle_filename = searchword + '_' + now.strftime('%Y%m%d') + '.pickle'  # 피클 파일 이름 설정
    pickle_buffer = io.BytesIO()
    list_pandas.to_pickle(pickle_buffer)  # 피클 파일로 저장
    pickle_buffer.seek(0)

    object_name = object_nm + str(pickle_filename)
    s3.upload_fileobj(pickle_buffer, bucket_name, object_name)  # 클라우드에 업로드
    print(f'Uploaded {pickle_filename} to S3')


#드라이버 가져오는 함수
def crawal_get_driver(url, view_case):
    if view_case == 'selenium':
        options = webdriver.ChromeOptions()
        options.add_argument("--allow-running-insecure-content")
        options.add_argument("--log-level=3")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        
        driver = webdriver.Chrome(service=Service(), options=options)
        driver.get(url)
        return driver

    elif view_case == 'requests':
        headers = {
            "User-Agent": 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
            "Accept-Language": "ko-KR,ko;q=0.8,en-US;q=0.5,en;q=0.3",
            "Accept-Encoding": "gzip"
        }


        res = requests.get(url, headers=headers)
        return res


#리스트 가져오는 함수
def get_list(info_set, ctk_textbox):
    
    url = 'https://www.reuters.com/site-search/?query={}&date=past_year&offset=0'.format(info_set['searchword'])
    driver = crawal_get_driver(url, "selenium")

    pagetext = ''
    content_list = []
    #페이지 최대 설정하기(Tesla 기준)
    for page in range(1, 5):
        bsObj = bs4.BeautifulSoup(driver.page_source, "html.parser")
        articleObj = bsObj.find_all("li", {"class":"search-results__item__2oqiX"})

        try:
            this_pagetext = bsObj.find("span", {"class":"text__text__1FZLe text__dark-grey__3Ml43 text__regular__2N1Xr text__large__nEccO search-results__text__13FtQ"})
        except:
            this_pagetext = ''

        for key, value in enumerate(articleObj):
            try:
                info_kind = value.find("span", {"class": "text__text__1FZLe text__inherit-color__3208F text__inherit-font__1Y8w3 text__inherit-size__1DZJi"}).get_text()
                info_title = value.find("a", {"class": re.compile(r'text__text__1FZLe text__dark-grey__3Ml43')}).get_text()
                info_time = value.find("time", {"class": re.compile(r'text__text__1FZLe text__inherit-color__3208F')}).get_text()
                info_href = "https://www.reuters.com" + value.find("a", {"class": re.compile(r'text__text__1FZLe text__dark-grey__3Ml43')}).attrs['href']


                #최근 데이터는 날짜 없이 시간만 저장되어 있어 날짜로 바꿔서 넣기
                if re.match(r'\d{1,2}:\d{2} [APap][Mm] GMT\+\d', info_time):
                    # 현재 날짜를 YYYYMMDD 형식으로 가져옴
                    current_date = datetime.now().strftime('%B %#d, %Y')
                    # 현재 날짜를 info_time에 추가
                    info_time = current_date

                original_date = datetime.strptime(info_time, "%B %d, %Y")
                info_time_change = original_date.strftime("%Y-%m-%d")

                temp_contex = {}
                temp_contex['info_kind'] = info_kind
                temp_contex['info_title'] = info_title
                temp_contex['info_time'] = info_time_change
                temp_contex['info_url'] = info_href
                temp_contex['info_time_v2'] = info_time

                content_list.append(temp_contex)
                print(info_time)

            except Exception as e:
                print(e)
                print(value)
                break;

        if page == 1:
            pagetext = bsObj.find("span", {"class": "text__text__1FZLe text__dark-grey__3Ml43 text__regular__2N1Xr text__large__nEccO search-results__text__13FtQ"})
        elif pagetext == this_pagetext:
            break
        else:
            btn = driver.find_element(By.XPATH, '//*[@id="fusion-app"]/div/div[2]/div/div[2]/div[3]/button[2]')
            driver.execute_script("arguments[0].click();", btn)

    driver.close()

    content_all = []
    for key, value in enumerate(content_list):
        url = value['info_url']

        req = crawal_get_driver(url, "requests")
        bsObj = bs4.BeautifulSoup(req.text, "html.parser")
        contextObj = bsObj.find_all("p", {"class":"text__text__1FZLe text__dark-grey__3Ml43 text__regular__2N1Xr text__small__1kGq2 body__full_width__ekUdw body__small_body__2vQyf article-body__paragraph__2-BtD"})
        context = []
        for key_c, value_l in enumerate(contextObj):
            if len(value_l.get_text().strip()) > 0:
                context.append(value_l.get_text().strip())

        value['info_context'] = " ".join(context)
        value['press_name'] = info_set['press_name']
        value['searchword'] = info_set['searchword']

        content_all.append(value)

    column_origin_ = {}

    column_origin_['press_name'] = "언론사"
    column_origin_['searchword'] = "검색어"
    column_origin_['info_kind'] = "기사종류"
    column_origin_['info_title'] = "제목"
    column_origin_['info_time'] = "날짜"
    column_origin_['info_context'] = "기사내용"
    column_origin_['info_url'] = "링크"
   

    make_df_result(content_all, column_origin_)

    
#기사 날짜 형식이랑 맞추기
def change_date_format(date_str):
    original_date = datetime.strptime(date_str, "%Y%m%d")
    formatted_date = original_date.strftime("%Y-%m-%d")

    return formatted_date

#main 실행함수
def usa_news_main(info_set, ctk_textbox):
    info_set['d_start'] = change_date_format(info_set['d_start'])
    info_set['d_end'] = change_date_format(info_set['d_end'])

    get_list(info_set, ctk_textbox)

#인자 설정해서 main함수 실행시키기
def usa_news_crawling(searchword, press_name = 'REUTERS', 
                      d_start = (datetime.now()- timedelta(days=1)).strftime('%Y%m%d'), 
                      d_end = (datetime.now()- timedelta(days=2)).strftime('%Y%m%d')):
    
    info_set = {}
    info_set['press_name'] = press_name
    info_set['searchword'] = searchword
    info_set['d_start'] = d_start
    info_set['d_end'] = d_end
    usa_news_main(info_set, '')