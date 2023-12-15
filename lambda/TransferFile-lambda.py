
import json
import boto3
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    # TODO implement
    source_bucket = event['Records'][0]['s3']['bucket']['name']
    object_key = event['Records'][0]['s3']['object']['key']
    target_bucket = 'tranferred-oluwasore-open-weather-project'
    copy_source = {'Bucket': source_bucket,'Key': object_key} 
    # key,Bucket within a dictionary
    
    #get waiter
    waiter = s3_client.get_waiter('object_exists')
    waiter.wait(Bucket=source_bucket, Key= object_key)
    
    # To copy and transfer to the target bucket
    s3_client.copy_object(Bucket = target_bucket,Key=object_key,CopySource = copy_source )
    
    
    return {
        'statusCode': 200,
        'body': json.dumps('Copy completed successfully!')
    }
