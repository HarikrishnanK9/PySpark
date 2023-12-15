
from pyspark import SparkContext
sc=SparkContext(master='local',appName='march').getOrCreate()
rdd=sc.textFile('/home/harikrishnan/PycharmProjects/spark_march/sample')
rdd.foreach(print)

print("*"*100)

rdd1=rdd.flatMap(lambda x:x.split(' '))
rdd1.foreach(print)

vowels = 'aeiouAEIOU'

# rdd2 = rdd1.map(lambda x:[1 if vowels in x else 0])
# string = 'luminar technolab'
# for i in string:
#     print(i)

rdd2 = rdd1.map(lambda x:[1 if i in vowels else 0 for i in x])
rdd2.foreach(print)
