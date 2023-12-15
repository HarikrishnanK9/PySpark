from pyspark import SparkContext
sc = SparkContext(master='local',appName='march').getOrCreate()
rdd = sc.textFile('/home/harikrishnan/Downloads/sample4.txt')

print("*"*100)

rdd1 = rdd.map(lambda x:x.split(','))
rdd2 = rdd1.filter(lambda x:x[3]>='23')
rdd2.foreach(print)

print("*"*100)

rdd3 = rdd1.filter(lambda x:x[5]=="chennai")
rdd4 = rdd3.map(lambda x:(x[1],x[2],x[3],x[5]))
rdd4.foreach(print)
