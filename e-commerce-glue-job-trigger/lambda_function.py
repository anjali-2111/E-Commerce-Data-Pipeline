import json
import boto3 

glue_client = boto3.client('glue')

def lambda_handler(event, context):
    
    s3Client = boto3.client('s3')
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    object_key =  event['Records'][0]['s3']['object']['key']
    
    print(event)
    print(bucket_name)
    print(object_key)
    
    if object_key.startswith('transactions') and object_key.endswith('.csv'):
        # Process the file
        print(f"Processing file: {object_key}")
        if event['Records'][0]['eventName'].startswith('ObjectCreated'):
            job_name = 'e-commerce-test-new'
            response = glue_client.start_job_run(JobName=job_name, Arguments={'https://e-commerce122.s3.amazonaws.com/': f's3://{bucket_name}/{object_key}'})
            print(f'Started glue job run:{response["JobRunId"]}')
            
        else:
            print('Event is not for ObjectCreated')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }