#Print only the even numbers from 1 to 41
from pyspark import SparkContext
sc=SparkContext(master='local',appName='march').getOrCreate()
rdd= sc.parallelize([i for i in range(1,42) if i%2==0])
rdd.foreach(print)
