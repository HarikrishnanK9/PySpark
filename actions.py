from pyspark import SparkContext
sc = SparkContext(master='local',appName='march').getOrCreate()
rdd = sc.parallelize([i for i in range(1,21)])

#collect
lst = rdd.collect()
print(lst)

#take ====limit
lst = rdd.take(3)
print(lst)

#first()
lst =rdd.first()
print(lst)

