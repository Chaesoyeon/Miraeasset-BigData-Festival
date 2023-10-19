import boto3
import pandas as pd
import numpy as np
import pickle
import os
import io

#네이버 클라우드 플랫폼 Object Storage key
def s3_setting():
    service_name = 's3'
    endpoint_url = 'https://kr.object.ncloudstorage.com'
    region_name = 'kr-standard'
    #이는 네이버 클라우드 플랫폼 Object Storage에 접근하기 위한 인증 정보입니다.
    #Object Storage에서 제공하는 Access Key와 Secret Key를 사용합니다.
    #신규 API 인증키 생성을 통해 [인증키 관리]에서 확인할 수 있습니다.
    access_key = ''
    secret_key = ''

    s3 = boto3.client(service_name, endpoint_url=endpoint_url, aws_access_key_id=access_key,
                        aws_secret_access_key=secret_key)

    bucket_name = 'bucket1chaegpt'

    # list all in the bucket
    max_keys = 300
    return s3, bucket_name, max_keys


#국내기업리스트 불러오기
def get_comp_list():
    s3, bucket_name, _ = s3_setting()

    file_path = '기업리스트/국내기업리스트.csv'  # 폴더 내에 있는 파일 경로

    # S3에서 파일 내용 읽기
    response = s3.get_object(Bucket=bucket_name, Key=file_path)
    file_content = response['Body'].read()

    # BytesIO 객체로 파일 내용을 감싸고 CSV 파일을 읽음
    file_content_bytesio = io.BytesIO(file_content)
    df = pd.read_csv(file_content_bytesio, encoding='cp949')
    code_list = df['종목코드']

    return df, code_list


#해외기업리스트 불러오기
def get_uscomp_list():
    s3, bucket_name, _ = s3_setting()

    file_path = '기업리스트/해외기업리스트.csv'  # 폴더 내에 있는 파일 경로

    # S3에서 파일 내용 읽기
    response = s3.get_object(Bucket=bucket_name, Key=file_path)
    file_content = response['Body'].read()

    # BytesIO 객체로 파일 내용을 감싸고 CSV 파일을 읽음
    file_content_bytesio = io.BytesIO(file_content)
    df = pd.read_csv(file_content_bytesio)

    return df
