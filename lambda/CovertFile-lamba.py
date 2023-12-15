import json
import boto3
import pandas as pd
import io

s3_client = boto3.client('s3')
def lambda_handler(event, context): 
    # TODO implement
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    
    target_bucket = 'cleaned-oluwasore-open-weather-project'
    
    
    waiter = s3_client.get_waiter('object_exists')
    waiter.wait(Bucket=source_bucket,Key=object_key)
    
    response = s3_client.get_object(Bucket=source_bucket,Key=object_key)
    print(response)

    
    df = pd.read_csv(io.BytesIO(response['Body'].read()))
    
    selected_columns = ['CityName',
    'CityId',
    'Sunrise(local_time)',
    'Sunset(local_time)',
    'Time_of_record',
    'Longitude',
    'latitude',
    'Weather_description',
    'Wind_speed',
    'Wind_direction',
    'Pressure',
    'Humidity',
    'Temperature(C)',
    'Mininum_temperature(C)',
    'Maximum_temperature(C)'
    
    ]
    

    #csv data
    df_data = df[selected_columns]
    
    csv_data = df_data.to_csv(index=False)
    
    #upload to s3
    s3_client.put_object(Bucket=target_bucket,Key=object_key,Body =csv_data)
    
    
    
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('csv selected and uploaded to s3 successfully')
    }
