import pendulum
from airflow.decorators import dag, task
import pandas as pd
import numpy as np
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table, Column, BigInteger,String,UniqueConstraint, Float,Integer, inspect, Boolean

def create_table() -> None:
    metadata = MetaData()
    table = Table(
        "clear_flats_dataset",
        metadata,

        Column('id', BigInteger),
        Column('floor', Integer),
        Column('kitchen_area', Float),
        Column('is_apartment', Boolean),
        Column('total_area', Float),
        Column('price', BigInteger),

        Column('latitude', Float),
        Column('longitude', Float),
        Column('ceiling_height', Float),
        Column('flats_count', Integer),
        Column('floors_total', Integer),
    
        UniqueConstraint('id', name='clear_dataset')
    )
    
    db_conn = PostgresHook('destination_db').get_sqlalchemy_engine()
    if not inspect(db_conn).has_table(table.name): 
        metadata.create_all(db_conn)

def extract(**kwargs):
    ti = kwargs['ti']
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()
    
    sql_query = """
        SELECT * FROM combined_flats_buildings;
    """
    
    data = pd.read_sql(sql_query, conn)
    conn.close()

    ti.xcom_push('extracted_data', data)

def remove_outliers(df, columns, threshold=1.5):
    df_clean = df.copy()
    for col in columns:
        Q1 = df_clean[col].quantile(0.25)
        Q3 = df_clean[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - threshold * IQR
        upper_bound = Q3 + threshold * IQR
        df_clean = df_clean[(df_clean[col] >= lower_bound) & (df_clean[col] <= upper_bound)]
    
    return df_clean

def transform(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract', key='extracted_data')
    columns_to_drop = ['building_id','building_type_int','rooms','build_year','has_elevator','living_area','studio']
    df_clean = data.drop(columns=columns_to_drop)

    columns_with_outliers = ['kitchen_area','total_area', 'price','ceiling_height','latitude', 'longitude', 'floor','flats_count','floors_total','flats_count']
    without_outliers = remove_outliers(df_clean, columns_with_outliers)
    without_duplicates = without_outliers.drop_duplicates()

    ti.xcom_push('transformed_data', without_duplicates)


def load(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='transform', key='transformed_data')
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table="clear_flats_dataset",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['id'],
        rows=data.values.tolist()
    )