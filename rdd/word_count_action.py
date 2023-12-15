from pyspark import SparkContext
sc = SparkContext(master='local',appName='march').getOrCreate()
rdd = sc.textFile(('/home/harikrishnan/PycharmProjects/spark_march/demo1'))
rdd.foreach(print)

print("*"*100)

rdd1 = rdd.flatMap(lambda x:x.split(','))
rdd1.foreach(print)

count = rdd1.countByValue()
print(count)

count = rdd1.countByKey()
print(count)


