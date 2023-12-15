# Age mxm 2 employee fname,lname,age,loc

from pyspark import SparkContext
sc = SparkContext(master = 'local',appName='feb').getOrCreate()
rdd = sc.textFile('/home/harikrishnan/Downloads/sample4.txt')

#splitting
rdd1 = rdd.map(lambda x:x.split(','))

#sorting by age
rdd2 = rdd1.sortBy(lambda x:x[3],ascending=False)
rdd2.foreach(print)

#collect fname,lname,age,loc
rdd3 = rdd2.filter(lambda x: (x[1],x[2],x[3],x[5]))

lst = rdd3.take(2)
print(lst)

#Age min 1 employee fname,lname,age,loc


