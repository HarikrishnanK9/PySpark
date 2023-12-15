from pyspark import SparkContext
sc = SparkContext(master='local',appName='march').getOrCreate()

rdd=sc.parallelize([i for i in range(1,21)])
rdd.foreach(print)

rdd1 = rdd.map(lambda x:x+2)
rdd1.foreach(print)
