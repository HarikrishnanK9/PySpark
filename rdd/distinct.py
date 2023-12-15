#Distinct is used to remove duplicate values

from pyspark import SparkContext
sc = SparkContext(master='local',appName='march').getOrCreate()
rdd = sc.textFile('/home/harikrishnan/Downloads/customer5.txt')
rdd1 = rdd.distinct()
rdd1.foreach(print)
