from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from ingest.ingest_data import Ingest
import pandas as pd 
import json
import psycopg2
from sqlalchemy import create_engine


# df_cities = pd.read_csv('./dags/file_utils/kabupaten_kota_jawa_barat.csv')

host="localhost"
database="jabar_openweather"
user="ahyar"
password="shanpark203"
port=int(float('5432')) # in integer
db = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
def ingest_all_cities(ti):
    df_cities = pd.read_csv('./dags/file_utils/kabupaten_kota_jawa_barat.csv')
    responses_city = []
    response_dict = {}
    for x,city in df_cities.iterrows():
        response_city = Ingest.ingest_openweather(city["city_lon"], city["city_lat"])
        data = json.loads(response_city)
        print(city["city_lat"],city["city_lon"])
        weather_main = data["weather"][0]["main"]
        weather_description = data["weather"][0].get("description",None)
        weather_temp = data["main"].get("temp",None)
        weather_temp_feels_like = data["main"].get("feels_like",None)
        weather_pressure = data["main"].get("pressure",None)
        weather_humidity = data["main"].get("humidity",None)
        weather_sea_level = data["main"].get("sea_level",None)
        weather_grnd_level = data["main"].get("grnd_level",None)
        weather_wind_speed = data["wind"].get("speed",None)
        weather_wind_gust = data["wind"].get("gust",None)
        time_dim = data["dt"]
        time_sunrise = data["sys"].get("sunrise",None)
        time_sunset = data["sys"].get("sunset",None)
        response_dict ={
            "city_id":city["city_id"],
            "city_name":city["city_name"],
            "weather_main" : weather_main,
            "weather_description" :weather_description,
            "weather_temp": weather_temp,
            "weather_temp_feels_like" : weather_temp_feels_like,
            "weather_pressure" : weather_pressure,
            "weather_humidity" : weather_humidity,
            "weather_sea_level" : weather_sea_level,
            "weather_grnd_level" : weather_grnd_level,
            "weather_wind_speed" : weather_wind_speed,
            "weather_wind_gust" : weather_wind_gust,
            "time_dim" : time_dim,
            "time_sunrise" : time_sunrise,
            "time_sunset" : time_sunset
        }
        responses_city.append(response_dict)
    # df_dt = pd.DataFrame.from_dict(responses_city)
    # df_dt.to_csv('./outputtry.csv')
    ti.xcom_push(key='responses_city',value=responses_city)

def insert_to_stg_temperature(ti):
    # Retrieve the XCom values
    xcom_values = ti.xcom_pull(task_ids='ingest_all_cities_data',key='responses_city')
    data_f = pd.DataFrame.from_dict(xcom_values)
    data_temp = data_f[["city_name","weather_pressure","weather_humidity","weather_sea_level","weather_grnd_level"]]
    data_temp.to_csv('./data_temp.csv')
    data_temp.to_sql(
        name="stg_temperature",con=db,schema="staging",if_exists='append', index=False
    )
    
def insert_to_stg_wind(ti):
    xcom_values = ti.xcom_pull(task_ids='ingest_all_cities_data',key='responses_city')
    data_f = pd.DataFrame.from_dict(xcom_values)
    data_wind = data_f[["city_id","weather_wind_speed","weather_wind_gust","time_dim"]]
    data_wind.to_sql(
        name="stg_wind",con=db,schema="staging",if_exists='append', index=False
    )

def insert_to_stg_weather(ti):
    xcom_values = ti.xcom_pull(task_ids='ingest_all_cities_data',key='responses_city')
    data_f = pd.DataFrame.from_dict(xcom_values)
    data_weather = data_f[["city_id","weather_main","weather_description","weather_temp","weather_temp_feels_like"]]
    data_weather.to_sql(
        name="stg_weather",con=db,schema="staging",if_exists='append', index=False
    )
    data_weather.to_csv('weather.csv')


default_args = {
    'owner': 'ahyar',
    'retries': 5,
    'retry_delay': timedelta(minutes=8)
}



with DAG(
    dag_id='openweather_pipeline',
    default_args=default_args,
    description='this is our first dag',
    start_date=datetime(2024,1,1,2),
    schedule_interval='@daily'
) as dag:
    task1 = PythonOperator(
        task_id = 'ingest_all_cities_data',
        python_callable=ingest_all_cities,
        dag=dag
    )
    task2=PythonOperator(
        task_id='insert_to_stg_temperature',
        python_callable=insert_to_stg_temperature,
        dag=dag
    )
    task3=PythonOperator(
        task_id='insert_to_stg_wind',
        python_callable=insert_to_stg_wind,
        dag=dag
    )
    task4=PythonOperator(
        task_id='insert_to_stg_weather',
        python_callable=insert_to_stg_weather,
        dag=dag
    )
    # task3= PythonOperator(
    #     task_id='truncate_staging',
    #     python_callable='echo this is task 3 command'
    # )

# task1.set_downstream(task2)
# task1.set_downstream(task3)

task1 >> [task2,task3,task4]
# task2 >> task3

# task1 >> [task2,task3]