import pandas as pd
import pandera.pandas as pa
from pandera import Column, Check, DataFrameSchema
import glob
import os
from datetime import datetime

def create_schema():
    schema = DataFrameSchema(
        columns={
            'Цена': Column(float, checks=[Check.gt(10000), Check.lt(100000000)], nullable=True, coerce=True),
            'Площадь': Column(float, checks=[Check.gt(10), Check.lt(1000)], nullable=True, coerce=True),
            'Количество комнат': Column(int, checks=[Check.in_range(1, 10)], nullable=True, coerce=True),
            'Этаж': Column(int, checks=[Check.gt(0), Check.lt(100)], nullable=True, coerce=True),
            'Количество этажей в доме': Column(int, checks=[Check.gt(0), Check.lt(100)], nullable=True, coerce=True),
            'Адрес': Column(str, nullable=True),
            'Описание': Column(str, nullable=True),
            'S3_изображения': Column(object, nullable=True),
            'Количество_фото': Column(int, checks=[Check.in_range(0, 50)], nullable=True, coerce=True)
        },
        checks=[
            pa.Check(
                lambda df: (df['Этаж'].isna()) | (df['Количество этажей в доме'].isna()) | 
                           (df['Этаж'] <= df['Количество этажей в доме']),
                name="floor_less_than_total"
            ),
        ],
        strict=False,
        coerce=True
    )
    return schema

def convert_types(df):
    df = df.copy()
    numeric_cols = ['Цена', 'Площадь', 'Этаж', 'Количество этажей в доме', 'Количество комнат', 'Количество_фото']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    return df

def validate_and_filter(df, schema):
    try:
        valid_df = schema.validate(df, lazy=True)
        return valid_df
    except pa.errors.SchemaError:
        print("Фильтрация некорректных данных...")
        
        valid_mask = pd.Series([True] * len(df))
        
        if 'Площадь' in df.columns:
            valid_mask &= (df['Площадь'].isna()) | ((df['Площадь'] >= 10) & (df['Площадь'] <= 1000))
        
        if 'Количество комнат' in df.columns:
            valid_mask &= (df['Количество комнат'].isna()) | ((df['Количество комнат'] >= 1) & (df['Количество комнат'] <= 10))
        
        if 'Адрес' in df.columns:
            valid_mask &= (df['Адрес'].isna()) | (df['Адрес'].astype(str).str.len() >= 3)
        
        if 'Этаж' in df.columns and 'Количество этажей в доме' in df.columns:
            valid_mask &= (df['Этаж'].isna()) | (df['Количество этажей в доме'].isna()) | \
                          (df['Этаж'] <= df['Количество этажей в доме'])
        
        valid_df = df[valid_mask].copy()
        print(f"Отфильтровано {len(df) - len(valid_df)} записей")
        print(f"Осталось {len(valid_df)} валидных записей")
        return valid_df

def save_clean_dataset(df, output_dir="data/final"):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{output_dir}/flats_clean_{timestamp}.parquet"
    df.to_parquet(filename, index=False)
    print(f"Сохранено: {filename}")
    return filename

files = glob.glob("data/processed/flats_with_photos_*.csv")
latest_file = sorted(files)[-1]

df = pd.read_csv(latest_file)
if 'Изображения' in df.columns:
    df = df.drop('Изображения', axis=1)

df = convert_types(df)
schema = create_schema()
valid_df = validate_and_filter(df, schema)

if len(valid_df) > 0:
    save_clean_dataset(valid_df)
else:
    print("Нет валидных данных для сохранения!")