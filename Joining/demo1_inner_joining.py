import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
ss=SparkSession.builder.master("local[1]").appName("march").getOrCreate()
df1=ss.read.csv("/home/harikrishnan/Downloads/Hive_Joining/custom.txt",sep=',',header=None,inferSchema=True)
df2=ss.read.csv("/home/harikrishnan/Downloads/Hive_Joining/order.txt",sep=',',header=None,inferSchema=True)

df3=df1.withColumnRenamed("_c0","Id").withColumnRenamed("_c1","name") \
    .withColumnRenamed("_c2","age").withColumnRenamed("_c3","location") \
    .withColumnRenamed("_c4","salary")
df4 = df2.withColumnRenamed("_c0","oid").withColumnRenamed("_c1","dat") \
    .withColumnRenamed("_c2","Id").withColumnRenamed("_c3","amount")

df3.show()
print("*"*100)
df4.show()
print("*"*100)
df5 = df3.join(df4,df3.Id==df4.Id,'inner')

df5.show()

