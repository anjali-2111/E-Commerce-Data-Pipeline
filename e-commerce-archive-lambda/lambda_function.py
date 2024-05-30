import boto3
import json

source_bucket = 'e-commerce122'
destination_bucket = 'e-commerce-archive11'


def move_files(s3, source_bucket, destination_bucket):
    for obj in s3.list_objects_v2(Bucket=source_bucket)['Contents']:
        source_key = obj['Key']
        destination_key = source_key

        s3.copy_object(CopySource={'Bucket': source_bucket, 'Key': source_key}, Bucket=destination_bucket, Key=destination_key)
        print(f"Moved {source_key} to {destination_bucket}/{destination_key}")


def lambda_handler(event, context):
    print(f"Event: {event}")

    # Extracting msg from sns notification
    msg = json.loads(event['Records'][0]['Sns']['Message'])
    details = msg['detail']
    glue_job_status = details['state']

    print(f"Glue Job Status: {glue_job_status}")

    if glue_job_status == "SUCCEEDED":
        if not source_bucket:
            print("No source_bucket")
            return {
                'statusCode': 400,
                'body': json.dumps('Missing source bucket name in event')
            }

        s3 = boto3.client('s3')

        try:
            move_files(s3, source_bucket, destination_bucket)
            print("Source and destination buckets exist:", source_bucket, destination_bucket)
            return {
                'statusCode': 200,
                'body': json.dumps('Files moved successfully!')
            }

        except Exception as e:
            print(f"Exception occurred: {e}")
            return {
                'statusCode': 500,
                'body': json.dumps(f'Error transferring files: {e}')
            }

    else:
        print("Failed Glue Job")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Glue job {glue_job_status} : {event}')
        }
