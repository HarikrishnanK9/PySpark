
from pyspark import SparkContext
sc=SparkContext(master='local',appName='march').getOrCreate()

rdd=sc.textFile('/home/harikrishnan/Downloads/sample4.txt')
rdd.foreach(print)

print("*"*75)

rdd1=sc.textFile('/home/harikrishnan/Downloads/customer')
rdd1.foreach(print)
