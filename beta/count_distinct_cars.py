from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark import SQLContext
import time

sc = SparkContext("local[8]", "db2-project-data-statistics")
sqlContext = SQLContext(sc)

log_records = sqlContext.read.load("db2_project_data.csv", format='com.databricks.spark.csv', header='true', inferSchema='true')

st = time.time()
num = log_records.select('ID').distinct().count()
print ('The number of distinct cars is: ', num)
print ('Operation took:', time.time() - st ,' seconds to complete.')

sc.stop()