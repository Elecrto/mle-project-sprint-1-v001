import pendulum
from airflow.decorators import dag, task
import pandas as pd
import numpy as np
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import MetaData, Table, Column, BigInteger,String,UniqueConstraint, Float,Integer, inspect, Boolean

def create_table() -> None:
    metadata = MetaData()
    combined_flats_buildings = Table(
        "combined_data",
        metadata,

        Column('id', BigInteger),
        Column('building_id', Integer),
        Column('floor', Integer),
        Column('kitchen_area', Float),
        Column('living_area', Float),
        Column('rooms', Integer),
        Column('is_apartment', Boolean),
        Column('studio', Boolean),
        Column('total_area', Float),
        Column('price', BigInteger),

        Column('build_year', Integer),
        Column('building_type_int', Integer),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('ceiling_height', Float),
        Column('flats_count', Integer),
        Column('floors_total', Integer),
        Column('has_elevator', Boolean),
    
        UniqueConstraint('id', name='combined_flats_data')
    )
    
    db_conn = PostgresHook('destination_db').get_sqlalchemy_engine()
    if not inspect(db_conn).has_table(combined_flats_buildings.name): 
        metadata.create_all(db_conn)

def extract(**kwargs):
    ti = kwargs['ti']
    hook = PostgresHook('destination_db')
    conn = hook.get_conn()
    
    sql_query = """
        SELECT 
            f.*,
            b.build_year,
            b.building_type_int,
            b.latitude,
            b.longitude,
            b.ceiling_height,
            b.flats_count,
            b.floors_total,
            b.has_elevator
        FROM flats AS f
        LEFT JOIN buildings AS b ON f.building_id = b.id
    """
    
    data = pd.read_sql(sql_query, conn)
    conn.close()

    ti.xcom_push('extracted_data', data)

def load(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract', key='extracted_data')
    hook = PostgresHook('destination_db')
    hook.insert_rows(
        table="combined_flats_buildings",
        replace=True,
        target_fields=data.columns.tolist(),
        replace_index=['id'],
        rows=data.values.tolist()
    )