from datetime import datetime, timedelta

from time import sleep
import datetime
import io
import boto3
import selenium
from selenium import webdriver
import time
import bs4
from bs4 import BeautifulSoup as bs
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
from urllib.request import urlopen
from urllib.error import URLError, HTTPError

from webdriver_manager.chrome import ChromeDriverManager
from ast import literal_eval
from urllib import parse
from s3_setting import *
import pandas as pd

def daum_news_crawling(comp):
    # ConnectionError방지(헤더설정)
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36'
    }

    media_company_list = []
    search_list = []
    Sentence_list = []
    Title_list = []
    Date_list = []
    URL_list = []

    page_num = 1

    while page_num < 5:  # 페이지 수정
        url = f'https://search.daum.net/search?w=news&nil_search=btn&DA=NTB&enc=utf8&cluster=y&show_dns=1&q={comp}&p={page_num}'
        page = requests.get(url, headers=headers)
        sleep(1)
        soup = bs(page.text, "lxml")
        body = soup.select('ul.c-list-basic')

        for i in body:
            hrefs = i.select('div.item-title')
            media_coms = i.select('div.c-tit-doc')

            for i in hrefs:
                #링크 저장
                href = i.select_one('a')['href']
                URL_list.append(href)

                page = requests.get(href, headers=headers)
                sleep(1)
                cont_soup = bs(page.text, 'lxml')
                cont_body = cont_soup.select('div.main-content')

                for j in cont_body:
                    #뉴스 본문에서 내용, 제목, 날짜 가져오기
                    Sentence = j.select_one('div.article_view').text
                    Title = j.select_one('h3.tit_view').text
                    Date = j.select_one('span.num_date').text

                    Sentence_list.append(Sentence)
                    Title_list.append(Title)
                    Date_list.append(Date)
            #신문사명 가져오기
            for k in media_coms:
                media_com = k.select('strong.tit_item')
                for l in media_com:
                    media = l.text
                    media = re.sub(r"\s", "", media)
                    media_company_list.append(media)
                    search_list.append(comp)

        print('다음 - 검색어: ' + comp + ' 페이지 번호: ' + str(page_num) + ' Page')
        page_num = page_num + 1

    # 결과 데이터프레임 생성
    result_final = {
        '검색어': search_list,
        '신문사명': media_company_list,
        '제목': Title_list,
        '날짜': Date_list,
        '기사내용': Sentence_list,
        '링크': URL_list
    }

    news_df = pd.DataFrame(result_final)
    news_df = news_df.drop_duplicates()
    
    
    #클라우드에 업로드 시작
    s3, bucket_name, _ = s3_setting()
    
    # 폴더 생성
    object_nm = str(datetime.datetime.now().strftime('%Y%m%d')) + '/daum_news_crawling/'
    
    now = datetime.datetime.now()
    
    pickle_filename = comp + '_' + now.strftime('%Y%m%d') + '.pickle'  # 피클 파일 이름 설정
    pickle_buffer = io.BytesIO()
    news_df.to_pickle(pickle_buffer)  # 피클 파일로 저장
    pickle_buffer.seek(0)

    object_name = object_nm + str(pickle_filename)
    s3.upload_fileobj(pickle_buffer, bucket_name, object_name)  # 클라우드에 업로드
    print(f'Uploaded {pickle_filename} to S3')
