import findspark
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import *
from pyspark import StorageLevel
import time

def getColumns(row, indexes):
    row = row.split(',')
    tmp = [row[i] for i in indexes]
    return tmp

findspark.init()

conf = (SparkConf()\
.setMaster("local[*]")\
.setAppName("db2-q2-spark-rdd-c")\
.set("spark.executor.memory", "3g")\
.set("spark.driver.memory", "5g"))

sc = SparkContext(conf = conf)

lines = sc.textFile("db2_project_data.csv")

consumption = lines.map(lambda s: getColumns(s, [0,2]))
header = consumption.first()
consumption = consumption.filter(lambda line : line != header)


dataset = sc.parallelize(consumption.collect())
slices = 50
# partitionBy executes Hash Partitioning by default
wp = dataset.partitionBy(slices) 
# Make it persistent on both MEMORY (if it "fits") and DISK 
# to spped-up computations at the cost of memory
wp.persist(StorageLevel.MEMORY_AND_DISK)

st = time.time()
tplRes = (0,0) # As of Python3, you can't pass a literal sequence to a function.
result = consumption.mapValues(lambda v: (v, 1))\
.reduceByKey(lambda a,b: ((float(a[0])+float(b[0]), float(a[1])+float(b[1]))))\
.mapValues(lambda v: v[0]/v[1])
fn = time.time() - st

result.persist(StorageLevel.MEMORY_AND_DISK)
print ('Query Computation Time:', fn, ' seconds.')
print ('Average Distance (Kilometers) per Customer [Using Hash Partitioning]:')
print (result.take(10))
print ('Cumulative Computation Time:', time.time() - st, ' seconds.')

sc.stop()