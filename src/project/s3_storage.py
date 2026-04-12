import boto3
from botocore.client import Config
import glob
import os
import pandas as pd
import requests
from datetime import datetime
import ast
import time

S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin"
BUCKET_NAME = "realty-images"


s3_client = boto3.client(
    "s3",
    endpoint_url=S3_ENDPOINT,
    aws_access_key_id=S3_ACCESS_KEY,
    aws_secret_access_key=S3_SECRET_KEY,
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)


try:
    buckets = s3_client.list_buckets()
    print("Доступные бакеты:")
    for bucket in buckets['Buckets']:
        print(f"  - {bucket['Name']}")
except Exception as e:
    print(f"Ошибка подключения: {e}")


files = glob.glob("data/raw/flats_raw_*.csv")
latest_file = sorted(files)[-1]

df = pd.read_csv(latest_file)


def parse_images(x):
    if pd.isna(x) or x == '[]' or x == '':
        return []
    try:
        return ast.literal_eval(x)
    except:
        return []
        
if 'Изображения' in df.columns:
    df['Изображения'] = df['Изображения'].apply(parse_images)

headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
        'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
        'Referer': 'https://realty.yandex.ru/',
        'Sec-Fetch-Dest': 'image',
        'Sec-Fetch-Mode': 'no-cors',
        'Sec-Fetch-Site': 'same-site',
    }
'''
def upload_image(image_url, flat_id, idx):
    if not image_url or image_url == '[]' or image_url == '':
        print(f"Пропуск: пустой URL")
        return None
    image_url = 'https://' + image_url
    response = requests.get(image_url, headers=headers, timeout=10)
    if response.status_code == 200:
        key = f"flats/{flat_id}/image_{idx}.jpg"
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=key,
            Body=response.content,
            ContentType="image/jpeg"
        )
        return f"s3://{BUCKET_NAME}/{key}"
'''
def upload_image(image_url, flat_id, idx):
    if not image_url or image_url == '[]' or image_url == '':
        print(f"Пропуск: пустой URL")
        return None
    
    try:
        image_url = 'https://' + image_url
        response = requests.get(image_url, headers=headers, timeout=15)
        
        if response.status_code == 200:
            key = f"flats/{flat_id}/image_{idx}.jpg"
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=key,
                Body=response.content,
                ContentType="image/jpeg"
            )
            return f"s3://{BUCKET_NAME}/{key}"
        else:
            print(f"HTTP {response.status_code}")
            return None
            
    except requests.exceptions.Timeout:
        print(f"Таймаут")
        return None
    except requests.exceptions.InvalidURL as e:
        print(f"Некорректный URL: {e}")
        return None
    except Exception as e:
        print(f"Ошибка: {e}")
        return None

    
def upload_all_images(flats_df):
    flats_df['S3_изображения'] = [[] for _ in range(len(flats_df))]
    flats_df['Количество_фото'] = 0
    for idx, row in flats_df.iterrows():
        flat_id = idx
        images = row.get('Изображения', [])
        print(idx)
            
        s3_uris = []
        for i, img_url in enumerate(images, 1):
            uri = upload_image(img_url, flat_id, i)
            if uri:
                s3_uris.append(uri)
            time.sleep(1)
            
        flats_df.at[idx, 'S3_изображения'] = s3_uris
        flats_df.at[idx, 'Количество_фото'] = len(s3_uris)
        
    return flats_df

df = upload_all_images(df)

os.makedirs("data/processed", exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
output_file = f"data/processed/flats_with_photos_{timestamp}.csv"
df.to_csv(output_file, index=False)

