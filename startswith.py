from pyspark import SparkContext
sc=SparkContext(master='local',appName='march').getOrCreate()
rdd=sc.textFile('/home/harikrishnan/PycharmProjects/spark_march/sample')
rdd.foreach(print)

print("*"*100)

rdd1 = rdd.map(lambda x:x.startswith('U'))
rdd1.foreach(print)
