import json
import time
import boto3
from kafka import KafkaConsumer
s3 = boto3.client('s3',
                  endpoint_url='http://host.docker.internal:9002',
                  aws_access_key_id='xxx',
                  aws_secret_access_key='xxx')
bucket_name = 'bronze'
consumer = KafkaConsumer(
    'stock_quotes',
    bootstrap_servers='host.docker.internal:29092',
    enable_auto_commit=True,
    auto_offset_reset='earliest',
    group_id='bronze-consumers',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

print("consumer streaming and saving to MinIO...")

for message in consumer:
    record = message.value
    symbol = record.get("symbol")
    ts = record.get("timestamp", int(time.time()))
    key = f"{symbol}/{ts}.json"

    s3.put_object(
        Bucket=bucket_name,
        Key=key,
        Body=json.dumps(record).encode('utf-8'),
        ContentType='application/json'
    )
    print(f"Saved record for {symbol} = s3://{bucket_name}/{key}")
