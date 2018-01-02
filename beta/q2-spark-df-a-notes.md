  * Approach #2 - Using Lambda & RDD Operations **(SLOW AF)**

```python
st = time.time()

resTuple = (0, 0)
rddTmp = log_records.select(['region_id', 'ID']).rdd

rowNum = rddTmp.count()
#print (rddTmp.take(5))

rddTmp = rddTmp.aggregateByKey(resTuple, lambda a,b: (a[0] + 1, rowNum), lambda a,b: (a[0] + b[0], rowNum))
rddRes = rddTmp.mapValues(lambda v: v[0]/float(v[1])*100).collect()

#print (rddRes.take(5))
print ('Operation took:', time.time() - st ,' seconds to complete.')
```