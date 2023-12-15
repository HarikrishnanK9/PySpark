from pyspark import SparkContext
sc=SparkContext(master='local',appName='march').getOrCreate()
rdd1=sc.textFile("/home/harikrishnan/PycharmProjects/spark_march/spark sql")
rdd2=sc.textFile("/home/harikrishnan/PycharmProjects/spark_march/spark_core")
rdd3=sc.textFile("/home/harikrishnan/PycharmProjects/spark_march/spark_graphx")
rdd4=sc.textFile("/home/harikrishnan/PycharmProjects/spark_march/spark_machine_learning")
rdd5=sc.textFile("/home/harikrishnan/PycharmProjects/spark_march/spark_streaming")
lst=[rdd1,rdd2,rdd3,rdd4,rdd5]
for j in lst:
    words1=j.flatMap(lambda x:x.split(' '))
    word_count1=words1.countByValue()
    for i,count in word_count1.items():
        print("{}:{}".format(i,count))
    print("*"*100)

# count = rdd1.countByValue()

# for j in lst:
#     words1=j.flatMap(lambda x:x.split(' '))
#     word_count1=words1.countByValue()
#     for i,count in word_count1.items():
#         print("{}:{}".format(i,count)

