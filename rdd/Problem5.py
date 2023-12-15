
from pyspark import SparkContext
sc = SparkContext(master='local',appName='march').getOrCreate()
rdd = sc.textFile('/home/harikrishnan/Downloads/sample.txt')

rdd1  = rdd.flatMap(lambda x:x.split(' '))
rdd2 = rdd1.map(lambda x:(x,1))
rdd3 = rdd2.reduceByKey(lambda x,y:x+y)
rdd3.foreach(print)
