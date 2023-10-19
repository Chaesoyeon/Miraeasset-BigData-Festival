from datetime import datetime, timedelta

from time import sleep
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

#미국 종목토론방 크롤링
def stocktwits_crawling(comp):
    count_of_review = 50
    driver = webdriver.Chrome()
    link = 'https://stocktwits.com/symbol/' + comp

    try:
        driver.get(link)
    except Exception as e:
        print(f"에러 발생: {str(e)}")
        driver.close()    # 에러가 발생하면 WebDriver를 종료하고
        #continue  # 다음 항목으로 이동

    driver.implicitly_wait(60)
    sleep(1)

    try:
        element = driver.find_element(By.XPATH,'//*[@id="Layout"]/div[3]/div[3]/div/div/div[1]/div[2]')
    except Exception as e:
        print(f"에러 발생: {str(e)}")
        driver.close()    # 에러가 발생하면 WebDriver를 종료하고
        #continue  # 다음 항목으로 이동
    
    driver.execute_script("arguments[0].scrollIntoView(true);", element)      
    sleep(1)
    try:
        driver.find_element(By.XPATH,'//*[@id="react-tabs-0"]').click()
        sleep(1)
    except:
        print('넘어감')
    #스크롤을 계속 내리며 크롤링을 진행(로딩이 안 되면 무한로딩 -> 다음으로 넘어가기)
    while True:
        loction = driver.find_elements(By.CLASS_NAME,'StreamMessage_heading__Yw4XG.flex.items-center')
        driver.execute_script("arguments[0].scrollIntoView(true);", loction[len(loction)-1])      
        if len(loction) >= int(count_of_review):
            sleep(1)
            break
        prev_height = driver.execute_script("return document.body.scrollHeight")
        loction = driver.find_elements(By.CLASS_NAME,'StreamMessage_heading__Yw4XG.flex.items-center')
        driver.execute_script("arguments[0].scrollIntoView(true);", loction[len(loction)-1])    
        sleep(3)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == prev_height:
            print("더 이상 스크롤할 수 없음")
            break
        prev_height = new_height

            
    reply_list = []
    date_list = []
    division_list = []
    html = driver.page_source
    soup = bs(html,'html.parser')
    for i in soup:
        titles = i.select('div.RichTextMessage_body__Fa2W1.whitespace-pre-wrap')  
        dates = i.select('div.StreamMessage_heading__Yw4XG.flex.items-center')  
        for i in titles:
            title = i.text
            reply_list.append(title)
        for i in dates:
            date= i.select('a')    
            if len(date) == 2:
                date_division = date[1]
                division_list.append('댓글')
            else:
                date_division = date[0]
                division_list.append('대댓글')
            for i in date_division:
                write_date = i.text
                date_list.append(write_date)

    driver.close()            

    result = {'날짜':date_list,'내용':reply_list,'구분':division_list}
    df = pd.DataFrame(dict( [ (k,pd.Series(v)) for k,v in result.items() ]))  

    
    #클라우드에 업로드 시작
    s3, bucket_name, _ = s3_setting()
    
    # 폴더 지정
    object_nm = str(datetime.now().strftime('%Y%m%d')) + '/stocktwits_crawling/'
    
    now = datetime.now()
    
    pickle_filename = comp + '_' + now.strftime('%Y%m%d') + '.pickle'  # 피클 파일 이름 설정
    pickle_buffer = io.BytesIO()
    df.to_pickle(pickle_buffer)  # 피클 파일로 저장
    pickle_buffer.seek(0)

    object_name = object_nm + str(pickle_filename)
    s3.upload_fileobj(pickle_buffer, bucket_name, object_name)  # 클라우드에 업로드
    print(f'Uploaded {pickle_filename} to S3')
