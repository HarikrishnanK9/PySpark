from pyspark import SparkContext
sc=SparkContext(master='local',appName='march').getOrCreate()
rdd = sc.textFile('/home/harikrishnan/PycharmProjects/spark_march/sample')
rdd.foreach(print)
