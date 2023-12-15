import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
ss=SparkSession.builder.master("local[1]").appName("march").getOrCreate()
df=ss.read.csv("/home/harikrishnan/Downloads/customer",sep=',',header=None)
# df.show()
df1=df.withColumnRenamed("_c0","Id").withColumnRenamed("_c1","fname") \
       .withColumnRenamed("_c2","lname").withColumnRenamed("_c3","age") \
       .withColumnRenamed("_c4","prof").withColumnRenamed("_c5","location")
df1.show()

print("*"*100)

df2 = df1.orderBy("age",ascending=False).select("fname","lname","age","prof","location")
df2.show(5)
print("1"*100)

df3 = df1.orderBy("age").select("fname","lname","age","prof")
df3.show(3)
print("*"*100)

df4 = df1.filter(col("location")=="india")
df4.show()
print("*"*100)

df5 = df1.filter(col("location")=="uk").orderby("age",ascending=False)
