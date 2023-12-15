from pyspark import SparkContext
sc=SparkContext(master='local',appName='march')
rdd=sc.parallelize([i for i in range(1,51)])

rdd1 = rdd.map(lambda x:(x,"small") if x<=15 else (x,"medium") if 16<=x<=40 else(x,"large"))
rdd1.foreach(print)
