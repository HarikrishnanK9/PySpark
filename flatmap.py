from pyspark import SparkContext
sc=SparkContext(master='local',appName='march').getOrCreate()
rdd=sc.textFile('/home/harikrishnan/PycharmProjects/spark_march/sample')
rdd.foreach(print)

print("*"*100)

rdd1 = rdd.map(lambda x:x.split(' '))
rdd1.foreach(print)

print("*"*100)

rdd2=rdd1.flatMap(lambda x:x)
rdd2.foreach(print)
