import findspark
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.sql.types import StringType
from pyspark import StorageLevel
from pyspark import SQLContext
import time

def getColumns(row, indexes):
    row = row.split(',')
    tmp = [row[i] for i in indexes]
    return tmp

findspark.init()

sc = SparkContext("local[*]", "db2-q2-spark-rdd-b")
sqlContext = SQLContext(sc)

lines = sc.textFile("db2_project_data.csv")

consumption = lines.map(lambda s: getColumns(s, [0,2]))
header = consumption.first()
consumption = consumption.filter(lambda line : line != header)
# Make it persistent on both MEMORY (if it "fits") and DISK 
# to spped-up computations at the cost of memory
consumption.persist(StorageLevel.MEMORY_AND_DISK)

st = time.time()
tplRes = (0,0) # As of Python3, you can't pass a literal sequence to a function.
result = consumption.mapValues(lambda v: (v, 1))\
.reduceByKey(lambda a,b: ((float(a[0])+float(b[0]), float(a[1])+float(b[1]))))\
.mapValues(lambda v: v[0]/v[1])
fn = time.time() - st

result.persist(StorageLevel.MEMORY_AND_DISK)
print ('Query Computation Time:', fn, ' seconds.')
print ('Average Distance (Kilometers) per Customer [Without Hash Partitioning]:')
print (result.take(10))
print ('Cumulative Computation Time:', time.time() - st, ' seconds.')

sc.stop()