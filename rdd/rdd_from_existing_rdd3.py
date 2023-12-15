

from pyspark import SparkContext
sc=SparkContext(master='local',appName='march')
rdd=sc.parallelize([i for i in range(1,31)])

rdd1=rdd.map(lambda x:'even' if x%2==0 else 'odd')
rdd1.foreach(print)
