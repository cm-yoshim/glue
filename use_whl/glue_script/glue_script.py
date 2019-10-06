from src import my_func
import json
import boto3


# parameter
bucket_name = '<your-bucket>'
s3_dir = 'glue/blog_whl/output/my_output.json'

# 結果を取得
result = my_func.get_dictionary()

# S3に結果を保存する
s3 = boto3.resource('s3')
obj = s3.Object(bucket_name, s3_dir)
result = obj.put(Body=json.dumps(result))
