#sample4.txt
#1) age equal to 21 fname,lname,age,phno
#2)chennai work fname,lname,age,phno
#3)age mxm 2 employee fname,lname,age,phno
#4)age minimum 1 employee full data
#5)chennai work ,age mxm1 employee fname,lname,age

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
ss=SparkSession.builder.master("local[1]").appName("march").getOrCreate()
df=ss.read.csv("/home/harikrishnan/Downloads/sample4.txt",sep=',',header=None)
df1=df.withColumnRenamed('_c0','Id').withColumnRenamed('_c1','fname') \
      .withColumnRenamed('_c2','lname').withColumnRenamed('_c3','age') \
      .withColumnRenamed('_c4','phno').withColumnRenamed('_c5','location')
df1.show()

print("*"*100)

df2 = df1.filter(col("age")==21).select("fname","lname","age","phno")
df2.show()
print("1"*100)

df3 = df1.filter(col("location")=="chennai").select("fname","lname","age","phno")
df3.show()
print("2"*100)

df4 = df1.orderBy("age",ascending=False).select("fname","lname","age","phno")
df4.show(2)
print("3"*100)

df5 = df1.orderBy("age").select("fname","lname","age","phno","location")
df5.show(1)
print("4"*100)

df6 = df1.filter(col("location")=="chennai").orderBy("age",ascending=False).select("fname","lname","age")
df6.show()
