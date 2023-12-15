# customer5.txt
#1each profession count
#2 Each profession total salary
#3 Each profession average salary
#4 Each profession max salary
#5 Each prof min

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
ss=SparkSession.builder.master("local[1]").appName("march").getOrCreate()
df=ss.read.csv("/home/harikrishnan/Downloads/customer5.txt",sep=',',header=None,inferSchema=True)

df1=df.withColumnRenamed("_c0","Id").withColumnRenamed("_c1","fname") \
    .withColumnRenamed("_c2","lname").withColumnRenamed("_c3","age") \
    .withColumnRenamed("_c4","prof").withColumnRenamed("_c5","location") \
    .withColumnRenamed("_c6","salary")

df2=df1.show(5)
#each profession count
print("*"*100)
df3 = df1.groupby("prof").count()
df3.show()
print("*"*100)

#Each profession total salary
df4 = df1.groupby("prof").sum("salary")
df4.show()
print("*"*100)

#Each location average salary
df5=df1.groupby("location").avg("salary")
df5.show()
print("*"*100)

#Each profession max salary
df6 = df1.groupby("prof").max("salary")
df6.show()
# Each prof min age
df7 =df1.groupby("prof").min("age")
df7.show()
