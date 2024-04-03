import json
import boto3
import pandas as pd


def lambda_handler(event, context):
    # TODO implement
    print(event)
    try:
        sns_client = boto3.client('sns')
        s3_client = boto3.client('s3')
        sns_arn = 'arn:aws:sns:us-east-1:590183878198:doordash-sns'
        bucket = event["Records"][0]["s3"]["bucket"]["name"]
        file =  event["Records"][0]["s3"]["object"]["key"]
        obj = s3_client.get_object(Bucket = bucket, Key=file)
        
        data = obj['Body'].read().decode("utf-8")
        
        df = pd.read_json(data)
            
        
        
        filtered_df = df[df['status'] == 'delivered']
        fjson = filtered_df.to_json(orient = 'records')
        print(filtered_df)
        target_file = 'doordash.json'
        target_bucket = 'doordash-target-pra'
        s3_client.put_object(
        Body=fjson,
        Bucket=target_bucket,
        Key=target_file
        )
        
        message = f"File {target_file} has been successfully placed at {target_bucket}"
        sns_client.publish(Subject="SUCCESS - File processed and uploaded successfully",
        TargetArn=sns_arn, 
        Message=message,
        MessageStructure='text'
            )
    
    except Exception as err:
        print(err)
        sns_client.publish(Subject="Failed - File processing failed",
        TargetArn=sns_arn, 
        Message="Processing failed",
        MessageStructure='text'
            )
    
    
    

    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
