from pyspark import SparkContext
sc=SparkContext(master='local',appName='march').getOrCreate()
rdd1=sc.textFile("/home/harikrishnan/PycharmProjects/spark_march/spark sql")
rdd2=sc.textFile("/home/harikrishnan/PycharmProjects/spark_march/spark_core")
rdd3=sc.textFile("/home/harikrishnan/PycharmProjects/spark_march/spark_graphx")
rdd4=sc.textFile("/home/harikrishnan/PycharmProjects/spark_march/spark_machine_learning")
rdd5=sc.textFile("/home/harikrishnan/PycharmProjects/spark_march/spark_streaming")


rdd6=rdd1.flatMap(lambda x:x.split(' ')).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False)
rdd6.foreach(print)
rdd7=rdd2.flatMap(lambda x:x.split(' ')).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False)
rdd7.foreach(print)
rdd8=rdd3.flatMap(lambda x:x.split(' ')).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False)
rdd8.foreach(print)
rdd9=rdd4.flatMap(lambda x:x.split(' ')).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False)
rdd9.foreach(print)
rdd10=rdd5.flatMap(lambda x:x.split(' ')).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False)
rdd10.foreach(print)

print("*"*100)

top3=rdd6.take(3)
print(top3)
top_3=rdd7.take(3)
print(top_3)
top__3=rdd8.take(3)
print(top__3)
to_p3=rdd9.take(3)
print(to_p3)
to__p3=rdd10.take(3)
print(to__p3)

print("*"*100)

rdd_union=rdd1.union(rdd2).union(rdd3).union(rdd4).union(rdd5)
rdd_union.foreach(print)

word_count = rdd_union.flatMap(lambda x:x.split(' ')).map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False)
word_count.foreach(print)
print("*"*100)

top5 = word_count.take(5)
print(top5)
