import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import *
from awsglue.dynamicframe import DynamicFrame
from datetime import datetime

## 0.パラメータ
args = getResolvedOptions(sys.argv,
                          ['OUTPUT_PATH',
                           'OUTPUT_FILE_FORMAT',
                           'OUTPUT_COMPRESSION_TYPE',
                           'glue_database',
                           'base_date',
                           'JOB_NAME'])

OUTPUT_PATH = args['OUTPUT_PATH']
OUTPUT_FILE_FORMAT = args['OUTPUT_FILE_FORMAT']
OUTPUT_COMPRESSION_TYPE = args['OUTPUT_COMPRESSION_TYPE']
glue_database = args['glue_database']
base_date = args['base_date']
base_date = base_date[1:11]


## 1.最初の設定
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## 1-1.inputデータの指定
profiles_source = glueContext.create_dynamic_frame.from_catalog(database = glue_database, table_name = "data", transformation_ctx = "profiles_source")


## 2.base_dateと実行時点の日付差を取得
## base_fileとの差分から、当日分のファイルを作成するために時間差を利用します。
## 2-1.日数差を計算する関数
def days_between(d1, d2):
    d1 = datetime.strptime(d1, "%Y-%m-%d")
    d2 = datetime.strptime(d2, "%Y-%m-%d")
    return (d2 - d1).days

## 2-2.実行時timestampの取得
now = datetime.now()
now = datetime.strftime(now,'%Y-%m-%d')

## 2-3.日数差から時間差を取得
date_diff = days_between(base_date, now)


## 3.DFに取り込む際の前処理
## 3-1.列のマッピング
applymapping_profiles = ApplyMapping.apply(frame = profiles_source, mappings = [("col0", "string", "datetime_5sec", "timestamp"), ("col1", "string", "datetime_minute", "timestamp"), ("col2", "string", "datetime_hour", "timestamp")], transformation_ctx = "applymapping_profiles")

## 3-2.曖昧なデータ型を解決する
resolvechoiceprofiles = ResolveChoice.apply(frame = applymapping_profiles, choice = "MATCH_CATALOG", database = glue_database, table_name = "data", transformation_ctx = "resolvechoiceprofiles0")

## 3-3.SQLで利用するためにテーブルを登録
profiles_df = resolvechoiceprofiles.toDF()
profiles_df.createOrReplaceTempView("profiles_temp_table") 


## 4.SQLの実行
## 4-1.実行するSQLの定義
## 単純にdate_addすると、データ型がdateになってしまいtimestamp部分が潰れてしまうので、時間差をintervalで追加する
spark_sql = """
SELECT 
substring(cast(datetime_5sec + INTERVAL {0} days as string), 1, 19) as datetime_5sec,
substring(cast(datetime_minute + INTERVAL {1} days as string), 1, 19) as datetime_min,
substring(cast(datetime_hour + INTERVAL {2} days as string), 1, 19) as datetime_hour
FROM profiles_temp_table
""".format(date_diff,date_diff,date_diff)

## 4-2.SQLの実行
output_df = spark.sql(spark_sql).orderBy('datetime_5sec', ascending=True)


## 5.実行結果の出力
consolidated_dynamicframe = DynamicFrame.fromDF(output_df.repartition(1), glueContext, "consolidated_dynamicframe")
datasink_output = glueContext.write_dynamic_frame.from_options(frame = consolidated_dynamicframe, connection_type = "s3", connection_options = {"path": OUTPUT_PATH,"compression": OUTPUT_COMPRESSION_TYPE}, format = OUTPUT_FILE_FORMAT, transformation_ctx = "datasink_output")
job.commit()