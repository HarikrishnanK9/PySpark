from pyspark import SparkContext
sc = SparkContext(master='local',appName='march')

rdd=sc.parallelize([i for i in range(1,11)])
rdd.foreach(print)

print("*"*100)

rdd1 = rdd.map(lambda x:(x*x))
rdd1.foreach(print)
