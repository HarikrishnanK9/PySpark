from pyspark import SparkContext
sc=SparkContext(master='local',appName='march').getOrCreate()
rdd = sc.parallelize([i for i in range(1,21)])
rdd1 = sc.parallelize([i for i in range(10,31)])

rdd3  = rdd.union(rdd1)
rdd3.foreach(print)

print("*"*100)

rdd4 = rdd.intersection(rdd1)
rdd4.foreach(print)
