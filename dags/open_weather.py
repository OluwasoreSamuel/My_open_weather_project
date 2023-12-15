from airflow import DAG
from datetime import timedelta,datetime
import json
import requests
import pandas as pd
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator

with open("/home/ubuntu/airflow/api.txt","r") as credentials:
    api_key = credentials.read()
print(api_key)


target_bucket = 'cleaned-oluwasore-open-weather-project'

def kelvin_to_celsius(temp_in_kelvin):
    temp_in_celsius = temp_in_kelvin - 273.15
    return round(temp_in_celsius,2)

def extract_weather_data():
    combined_df = pd.DataFrame()
    base_url = "https://api.openweathermap.org/data/2.5/weather?q="
    cities = ["Kaura Namoda","Damaturu","Jalingo","Sokoto","Port Harcourt","Jos",
            "Ibadan","Ilesha","Akure","Abeokuta","Minna","Nasarawa","Mushin",
            "Ilorin","Lokoja","Gwandu","Katsina","Kano","Dutse","Owerri","Gombe",
            "Abuja","Nsukka","Ado-Ekiti","Benin City",
            "Abakaliki","Warri","Calabar","Maiduguri","Makurdi","Brass",
            "Misau","Onitsha","Uyo","Yola","Umuahia"]
    for city in cities:
        endpoint_url = city + ",ng"+"&appid=" + api_key
        full_url = base_url + endpoint_url
        response = requests.get(full_url)
        data = response.json()
        
        city = data['name']
        city_id = data['id']
        sunrise_time = datetime.fromtimestamp(data['sys']['sunrise'] + data['timezone'])
        sunset_time = datetime.fromtimestamp(data['sys']['sunset'] + data['timezone'])
        country = data['sys']['country']
        time_of_record = datetime.fromtimestamp(data['dt'] + data['timezone'])
        wind_speed =  data['wind']['speed']
        wind_direction = data['wind']['deg']
        pressure = data['main']['pressure']
        humidity = data['main']['humidity']
        temperature = kelvin_to_celsius(data['main']['temp']) #create a function to kelvin to Celcius
        feels_like = kelvin_to_celsius(data['main']['feels_like'])
        min_temp = kelvin_to_celsius(data['main']['temp_min'])
        max_temp = kelvin_to_celsius(data['main']['temp_max']) # function ends here
        longitude = data['coord']['lon']
        latitude = data['coord']['lat']
        weather_description = data['weather'][0]['description']
        base = data['base']
        
        # convert into a dictionary
        transformed_data ={"CityName": city,
                    "CityId": city_id,
                    "Sunrise_local_time": sunrise_time,
                    "Sunset_local_time": sunset_time,
                    "Country": country,
                    "Time_of_record": time_of_record,
                    "Wind_speed": wind_speed,                   
                    "Wind_direction": wind_direction,
                    "Temperature": temperature,
                    "Feels_like": feels_like,
                    "Mininum_temperature": min_temp,
                    "Maximum_temperature": max_temp,
                    "Pressure": pressure,
                    "Humidity": humidity,
                    "Base": base,
                    "Weather_description": weather_description,
                    "Longitude": longitude,
                    "latitude": latitude,
                    
        }
        transformed_data_list = []
        transformed_data_list.append(transformed_data)
        df_data = pd.DataFrame(transformed_data_list)
        combined_df = pd.concat([combined_df,df_data],ignore_index= True)
        
    #to csv file
    now = datetime.now()
    date_now_string = now.strftime(("%d%m%Y%H%M%S"))
    dt_string = "Weather_data_nigeria_" + date_now_string
    output_file_path = f'/home/ubuntu/{dt_string}.csv'
    file_name = f'{dt_string}.csv'
    output_list = [output_file_path,file_name]
    combined_df.to_csv(f"{dt_string}.csv",index=False)
    return output_list

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023,12,11),
    'email': [],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=30)
    
}

with DAG('open_weather_dag',
         default_args= default_args,
         schedule_interval= timedelta(minutes= 10) ,
         catchup=False) as dag:
    
    extract_weather_data_var = PythonOperator (
        task_id = 'tsk_extract_weather_data',
        python_callable= extract_weather_data
    )
    load_to_s3_var = BashOperator(
        task_id = 'tsk_load_to_s3',
        bash_command = 'aws s3 mv {{ti.xcom_pull("tsk_extract_weather_data")[0]}} s3://oluwasore-open-weather-project',
    )
    is_file_available_s3_var = S3KeySensor(
        task_id = 'tsk_is_file_available_s3',
        bucket_key = '{{ti.xcom_pull("tsk_extract_weather_data")[1]}}',
        bucket_name = target_bucket,
        aws_conn_id = 'aws_s3_conn',
        wildcard_match = False,
        timeout = 60,
        poke_interval = 5,
    )
    transfer_s3_to_redshift_var = S3ToRedshiftOperator(
        task_id = 'tsk_transfer_s3_to_redshift',
        s3_bucket=target_bucket,
        aws_conn_id= 'aws_s3_conn',
        redshift_conn_id='aws_redshift_conn_id',
        s3_key='{{ti.xcom_pull("tsk_extract_weather_data")[1]}}',
        schema= "PUBLIC",
        table="NigeriaOpenWeather",
        copy_options= ["csv IGNOREHEADER 1"]
        
        
        
    )
    
    
    extract_weather_data_var>>load_to_s3_var>>is_file_available_s3_var>>transfer_s3_to_redshift_var