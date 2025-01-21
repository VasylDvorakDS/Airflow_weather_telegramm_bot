
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine
import os, sys
import requests
import pendulum
from airflow.decorators import dag, task
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import to_date, col
import pandas as pd
from airflow.providers.telegram.operators.telegram import TelegramOperator
from airflow import DAG
from airflow.operators.python import PythonOperator
from pandas.io import sql


os.environ["no_proxy"]="*"
con = create_engine("mysql://Airflow:1@localhost/spark")

# Параметры подключения к MySQL
jdbc_url = "jdbc:mysql://localhost:33061/spark"
properties = {
    "user": "Airflow",
    "password": "1",
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Установка конфигурации для Spark
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession.builder.appName("Hi").getOrCreate()


@dag(
    dag_id="wether-tlegram",
    schedule="@once",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)

def WetherETL():
    
    @task(task_id='send_message_weather_telegram_task')
    def send_message_weather_telegram_task(**kwargs):
            yandex_temp = kwargs['ti'].xcom_pull(task_ids='yandex_wether', key='wether')
            open_temp = kwargs['ti'].xcom_pull(task_ids='open_wether', key='open_wether')
            token='8020491492:AAGctnIlVxGDm78Bq90fmwInYtNuYwmCgpY'
            # Telegram API request
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            chat_id = "5333394756"

            message =f"""
    <b>Weather in Moscow</b>
    <pre>
| ||Source       | Degrees |
| |--------------|---------|
|1|Yandex        | {yandex_temp}      |
|2|Open Weather  | {open_temp}    |
    </pre>
            """
        
            params = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
            }

            response = requests.post(url, params=params)
            if response.status_code != 200:
                raise Exception(f"Failed to send message about weather: {response.text}")
            
            
    @task(task_id='send_message_credit_telegram_task')
    def send_message_credit_telegram_task(**kwargs):
            name_table = 'Without_repayment'
            df = kwargs['ti'].xcom_pull(task_ids='read_credit_table', key=name_table)
            token='8020491492:AAGctnIlVxGDm78Bq90fmwInYtNuYwmCgpY'
            # Telegram API request
            url = f"https://api.telegram.org/bot{token}/sendMessage"
            chat_id = "5333394756"
            
            headers_list = df.columns.tolist()
            
            message="<pre>|" 
            for header in headers_list:
                message +=" "*5+ f"{header}|"
            
            message += "\n"+"|"
            for header in headers_list:
                message += "-"*(len(header)+4)+":|"
                
            message += "\n"
            for index, row in df.iterrows():
                row = row.astype(str)       
                message +=f"|{' ' if len(row['№']) == 2 else '  '}   {row['№']}|{row['Месяц']}|           {row['Сумма платежа']}|                       {' ' if len(row['Платеж по основному долгу']) == 6 else ''}{row['Платеж по основному долгу']}|                 {row['Платеж по процентам']}|         {row['Остаток долга']}|    {' ' if len(row['проценты']) == 8 else '  ' if len(row['проценты']) == 7  else ''}{row['проценты']}|  {row['долг']}|\n"
            
            message += "|"    
            for header in headers_list:
                message += "-"*(len(header)+5)+"|"
            
            message+="</pre>"
            
            params = {
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "HTML"
            }

            response = requests.post(url, params=params)
            if response.status_code != 200:
                raise Exception(f"Failed to send message about credit: {response.text}")
            
            
            

            
    @task(task_id='yandex_openweather_wether_create_table')        
    def yandex_openweather_wether_create_table():
        name_table = 'yandex_openweather_wether_table'
        with con.connect() as connection:
            sql.execute(f"""
            CREATE TABLE IF NOT EXISTS spark.`{name_table}` (
                `№` INT(10) NOT NULL AUTO_INCREMENT,
                `City` TEXT NULL DEFAULT NULL,
                `Temperature_Yandex` FLOAT NULL DEFAULT NULL,
                `Temperature_OpenWeather` FLOAT NULL DEFAULT NULL,
                `Date_time_UTC` TIMESTAMP NULL DEFAULT NULL,
                `Time_zone` TEXT NULL DEFAULT NULL,
                PRIMARY KEY (`№`)
            )
            COLLATE='utf8mb4_0900_ai_ci'
            ENGINE=InnoDB
        """, connection) 
    
                  
    @task(task_id='send_temperature_to_mysql')
    def send_temperature_to_mysql(**kwargs):
        temperature_Yandex = float(kwargs['ti'].xcom_pull(task_ids='yandex_wether', key='wether'))
        temperature_OpenWeather = float(kwargs['ti'].xcom_pull(task_ids='open_wether', key='open_wether'))
        city_name = 'Moscow'
        timezone_seconds = kwargs['ti'].xcom_pull(task_ids='open_wether', key='time_zone')
        sign_time_zone = "+" if timezone_seconds >= 0 else "-"
        time_zone = sign_time_zone + f"{abs(timezone_seconds) // 3600:02}:{(abs(timezone_seconds) % 3600) // 60:02}"
        timestamp_str = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                
        with con.connect() as connection:
            sql.execute(f"""INSERT INTO spark.`yandex_openweather_wether_table` (City, Temperature_Yandex, Temperature_OpenWeather, Date_time_UTC, Time_zone) 
                                VALUES ('{city_name}', {temperature_Yandex}, {temperature_OpenWeather}, '{timestamp_str}', '{time_zone}')""", connection)
        

    @task(task_id='yandex_wether')
    def get_yandex_wether(**kwargs):
        ti = kwargs['ti']
        url = "https://api.weather.yandex.ru/v2/informers/?lat=55.75396&lon=37.620393"

        payload={}
        headers = {
        'X-Yandex-API-Key': '33f45b91-bcd4-46e4-adc2-33cfdbbdd88e'
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        print("test")
        a=response.json()['fact']['temp']
        print(a)
        ti.xcom_push(key='wether', value=response.json()['fact']['temp'])
#        return str(a)

    @task(task_id='open_wether')
    def get_open_wether(**kwargs):
        ti = kwargs['ti']
        url = "https://api.openweathermap.org/data/2.5/weather?lat=55.749013596652574&lon=37.61622153253021&appid=2cd78e55c423fc81cebc1487134a6300"

        payload={}
        headers = {}

        response = requests.request("GET", url, headers=headers, data=payload)
        print("test")
        a=round(float(response.json()['main']['temp']) - 273.15, 2)
        print(a)
        ti.xcom_push(key='open_wether', value=round(float(response.json()['main']['temp']) - 273.15, 2))
        ti.xcom_push(key='time_zone', value=response.json()['timezone'])
#        return str(a)

    @task(task_id='read_credit_table')
    def read_credit_table(**kwargs):
        name_table = 'Without_repayment'
        ti = kwargs['ti']
        df1 = spark.read.format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", name_table) \
            .options(**properties) \
            .load().limit(20)

        # Указываем формат даты
        df1 = df1.withColumn("Месяц", to_date(col("Месяц"), "yyyy-MM-dd"))
        df2 = df1.toPandas()  # Конвертация в Pandas DataFrame
        df2["Месяц"] = pd.to_datetime(df2["Месяц"], format="%Y-%m-%d", errors="coerce").dt.strftime('%Y-%m-%d')
        ti.xcom_push(key=name_table, value=df2)
    
    @task(task_id='python_wether')
    def get_wether(**kwargs):
        print("Yandex "+str(kwargs['ti'].xcom_pull(task_ids=['yandex_wether'],key='wether')[0])+" Open "+str(kwargs['ti'].xcom_pull(task_ids=['open_wether'],key='open_wether')[0]))
    
    get_yandex_wether()>> get_open_wether()>> get_wether()>> read_credit_table()>> yandex_openweather_wether_create_table()>> send_message_weather_telegram_task()>> send_temperature_to_mysql() >>send_message_credit_telegram_task()
dag = WetherETL()
