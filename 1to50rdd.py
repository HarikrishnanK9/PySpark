#Create an rdd with elements ranging from 1 to 50

# from pyspark import SparkContext
# sc=SparkContext(master='local',appName='march').getOrCreate()
# rdd = sc.parallelize([i for i in range(1,51)])
# rdd.foreach(print)


from pyspark import SparkContext
sc = SparkContext(master='local',appName='march').getOrCreate()
rdd = sc.parallelize([i for i in range(1,51)])
rdd.foreach(print)
