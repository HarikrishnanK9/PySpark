from pyspark import SparkContext
sc = SparkContext(master='local',appName='march').getOrCreate()
rdd = sc.parallelize([1,2,3,4,5,6,7,8,9,10])
rdd.foreach(print)
