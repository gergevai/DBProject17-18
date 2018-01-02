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
num = log_records.agg(sum(log_records['kilometers'])).collect()[0][0]
res = log_records.groupby(month(log_records['timestamp']).alias('month')).agg((sum(log_records['kilometers'])/num*100).alias('percentage'))
fn = time.time() - st

print ('Query Computation Time:', fn, ' seconds.')
print ('The kilometer Distance Percentage per month:')
print (res.show(3))
print ('Cumulative Computation Time:', time.time() - st, ' seconds.')

sc.stop()