import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema
import glob
import os
from datetime import datetime
import logging

def create_schema():
    schema = DataFrameSchema(
        columns={
            'Цена': Column(
                float,
                checks=[
                    Check.gt(10000, error="Цена должна быть больше 100 000 ₽"),
                    Check.lt(100000000, error="Цена должна быть меньше 100 000 000 ₽"),
                    Check(lambda x: x > 0, element_wise=True, error="Цена должна быть положительной")
                ],
                nullable=True,
                description="Цена в рублях"
            ),
            'Площадь': Column(
                float,
                checks=[
                    Check.gt(10, error="Площадь должна быть больше 10 м²"),
                    Check.lt(500, error="Площадь должна быть меньше 500 м²"),
                    Check(lambda x: x > 0, element_wise=True, error="Площадь должна быть положительной")
                ],
                nullable=True,
                description="Общая площадь в м²"
            ),
            'Количество комнат': Column(
                int,
                checks=[
                    Check.in_range(1, 5, error="Количество комнат должно быть от 1 до 5"),
                    Check(lambda x: x > 0, element_wise=True, error="Количество комнат должно быть положительным")
                ],
                nullable=True,
                description="Количество комнат"
            ),
            'Этаж': Column(
                int,
                checks=[
                    Check.gt(0, error="Этаж должен быть больше 0"),
                    Check.lt(100, error="Этаж должен быть меньше 100")
                ],
                nullable=True,
                description="Этаж квартиры"
            ),
            'Количество этажей в доме': Column(
                int,
                checks=[
                    Check.gt(0, error="Количество этажей должно быть больше 0"),
                    Check.lt(100, error="Количество этажей должно быть меньше 100")
                ],
                nullable=True,
                description="Всего этажей в доме"
            ),
            'Адрес': Column(
                str,
                checks=[
                    Check.str_length(min_value=5, max_value=500, error="Длина адреса должна быть от 5 до 500 символов")
                ],
                nullable=True,
                description="Адрес квартиры"
            ),
            'Описание': Column(
                str,
                nullable=True,
                description="Текстовое описание квартиры"
            ),
            'S3_изображения': Column(
                object,
                nullable=True,
                description="Список S3 URI загруженных изображений"
            ),
            'Количество_фото': Column(
                int,
                checks=[
                    Check.in_range(0, 50, error="Количество фото должно быть от 0 до 50")
                ],
                nullable=True,
                description="Количество загруженных изображений"
            )
        },
        checks=[
            pa.Check(
                lambda df: (df['Этаж'].isna()) | (df['Количество этажей в доме'].isna()) | 
                           (df['Этаж'] <= df['Количество этажей в доме']),
                name="floor_less_than_total",
                error="Этаж не может быть больше общего количества этажей"
            ),
            pa.Check(
                lambda df: (df['Количество_фото'] == 0) | (df['S3_изображения'].notna()),
                name="photos_consistency",
                error="Если есть фото, S3_изображения не должен быть пустым"
            )
        ],
        strict=False,
        coerce=True
    )
    
    return schema


def convert_types(df):
    df = df.copy()
    
    numeric_columns = ['Цена', 'Площадь', 'Этаж', 'Количество этажей в доме', 'Количество комнат', 'Количество_фото']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    string_columns = ['Адрес', 'Метро', 'Описание']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str) if df[col].notna().any() else df[col]
            df[col] = df[col].str.strip()

    return df


def validate_data(df, schema):
    errors = []
    
    try:
        valid_df = schema.validate(df, lazy=True)
        return valid_df, errors
        
    except pa.errors.SchemaError as e:
        if hasattr(e, 'failure_cases') and e.failure_cases is not None:
            for _, row in e.failure_cases.iterrows():
                errors.append({
                    'row_index': row.get('index', 'unknown'),
                    'column': row.get('column', 'unknown'),
                    'check': row.get('check', 'unknown'),
                    'failure_case': str(row.get('failure_case', '')),
                })
        
        valid_mask = pd.Series([True] * len(df))
        
        for col_name, col_schema in schema.columns.items():
            if col_name in df.columns:
                for check in col_schema.checks:
                    try:
                        if hasattr(check, '_element_wise') and check._element_wise:
                            result = df[col_name].apply(lambda x: check._check_fn(x) if pd.notna(x) else True)
                            valid_mask &= result
                        else:
                            result = check(df[col_name])
                            if isinstance(result, pd.Series):
                                valid_mask &= result
                    except Exception:
                        pass
        
        valid_df = df[valid_mask].copy()
        #invalid_count = len(df) - len(valid_df)
        
        return valid_df, errors


def save_clean_dataset(df, output_dir="data/final"):
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"{output_dir}/flats_clean_{timestamp}.parquet"
    
    df.to_parquet(filename, index=True)
    
    csv_filename = filename.replace('.parquet', '.csv')
    df.to_csv(csv_filename, index=True, encoding='utf-8-sig')
    
    return filename


files = glob.glob("data/processed/flats_with_photos_*.parquet")

latest_file = sorted(files)[-1]

try:
    df = pd.read_parquet(latest_file)
except Exception as e:
    print(f"Ошибка загрузки: {e}")
    
df = convert_types(df)
schema = create_schema()
valid_df, errors = validate_data(df, schema)
    
if len(valid_df) > 0:
    save_clean_dataset(valid_df)