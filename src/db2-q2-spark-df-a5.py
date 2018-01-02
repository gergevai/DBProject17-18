import findspark
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark import SQLContext
import time

findspark.init()

sc = SparkContext("local[*]", "db2-q2-spark-df-a")
sqlContext = SQLContext(sc)

log_records = sqlContext.read.load("db2_project_data.csv", format='com.databricks.spark.csv', header='true', inferSchema='true')
# Make it persistent to both MEMORY (if it "fits") and DISK to speed up computations
log_records.persist()

st = time.time()
res = log_records.groupby([log_records['region_id'], month(log_records['timestamp']).alias('month')]).agg(avg(log_records['kilometers']).alias('avg_kilometers'))
fn = time.time() - st

print ('Query Computation Time:', fn, ' seconds.')
print ('The Average Kilometer Distance in each Region per Month:')
print (res.show(3))
print ('Cumulative Computation Time:', time.time() - st, ' seconds.')

sc.stop()