from airflow import DAG
from datetime import timedelta, datetime
from airflow.providers.http.sensors.http import HttpSensor
import json
from airflow.providers.http.operators.http import HttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd


def kelvin_to_fahrenheit(temp_in_kelvin):
    temp_in_fahrenheit = (temp_in_kelvin - 273.15) * (9/5) + 32
    return temp_in_fahrenheit



def transform_load_data(task_instance):
    data = task_instance.xcom_pull(task_ids="extract_weather_data")
    city = data["name"]
    weather_description = data["weather"][0]['description']
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    time_of_record = datetime.utcfromtimestamp(data['dt'] + data['timezone'])
    sunrise_time = datetime.utcfromtimestamp(data['sys']['sunrise'] + data['timezone'])
    sunset_time = datetime.utcfromtimestamp(data['sys']['sunset'] + data['timezone'])

    transformed_data = {"City": city,
                        "Description": weather_description,
                        "Temperature (F)": temp_farenheit,
                        "Feels Like (F)": feels_like_farenheit,
                        "Minimun Temp (F)":min_temp_farenheit,
                        "Maximum Temp (F)": max_temp_farenheit,
                        "Pressure": pressure,
                        "Humidty": humidity,
                        "Wind Speed": wind_speed,
                        "Time of Record": time_of_record,
                        "Sunrise (Local Time)":sunrise_time,
                        "Sunset (Local Time)": sunset_time                        
                        }
    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)
    aws_credentials = {"key": "ASIAUGO4K6AB2VH4UP24", "secret": "NhFPURVBffbIpM6A6bE1+GG3NaoqdDayKDnCVv5l", "token": "IQoJb3JpZ2luX2VjEM7//////////wEaCmFwLXNvdXRoLTEiRzBFAiAe2HiCZOWOZzVX6uaiV6lSrK2uQbLx+r3Tvly16diojAIhALTOJzlYF341uZ+Dl0lH6Crc4MvC3eJDWGr7SF/dEGo8KtwBCLf//////////wEQABoMMjg4NzYxNzY5OTg3IgzVuZEOS9M3vf3l2NgqsAFJA/QowT+/HgLGc/+AYs6UJjBGq7JUGTphlnWN7aveHyjuiXt+9jdDKlrbduqXvMNMUiFfj4brX3E3ak4CzCdlKhqpT5dZg3yI+8HXQkaB6gZ/s1gGXkxgAv1Uz61hFR6lQzlL2NGJYJA13iZbMwyY3fvr6fAsmxPE8ulI4uFrRDAb2UvgRFIyjoN0BNfjsipuL9+todM2Dp4DsGGPpqsumN8kETY9uuYXRhg3+m55DDDDkIi8BjqYAX2pZdyDYrdgO7kOjNmvfrO1l6jb5W1C5FstvD2q7opzuaGBpaNir9nYrTFm7Can0ZeaF8MXFrnYTTD8KIoQSJVXIpiMUvkwMVv3AOzCgaXtqPHlR/U3eZMHnj8YapLD6ngvaGN5Ery6N1pFMN/zsfptJbRgRgxWPMo7+v6x6k2A+5VT42mK1NxHFbeqG+m5Pd8q8wVG+1vc"}


    now = datetime.now()
    dt_string = now.strftime("%d%m%Y%H%M%S")
    dt_string = 'current_weather_data_portland_' + dt_string
    df_data.to_csv(f"s3://weather-api-airflow-bucket-yml/{dt_string}.csv", index=False, storage_options=aws_credentials)







default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 4),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2)
}


with DAG('weather_dag',
        default_args=default_args,
        schedule_interval = '@hourly',
        catchup=False) as dag:


        is_weather_api_ready = HttpSensor(
        task_id ='is_weather_api_ready',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Portland&APPID=89f269dc002cd3f57fede53a09e8fd71'
        )

        extract_weather_data = HttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weathermap_api',
        endpoint='/data/2.5/weather?q=Portland&APPID=89f269dc002cd3f57fede53a09e8fd71',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
        )

        transform_load_weather_data = PythonOperator(
        task_id= 'transform_load_weather_data',
        python_callable=transform_load_data
        )



        is_weather_api_ready >> extract_weather_data >> transform_load_weather_data

        


