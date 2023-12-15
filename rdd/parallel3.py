from pyspark import SparkContext
sc = SparkContext(master = 'local',appName='march').getOrCreate()
rdd = sc.parallelize({"id":101,"fname":"Hari"})
rdd.foreach(print)

#dictionary rdd aakkiyal key matrame result aaai kanikkukayullu.athinte value output aai generate aakilla.mattellam correct kittum.list,tuple,set correct varum
