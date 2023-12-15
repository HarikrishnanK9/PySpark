#Evaluating function 1:COUNT()
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
#location count
df2 = df1.groupby("location").count().orderBy("count",ascending=False)
df2.show()
print("*"*100)
#profession count
df3 = df1.groupby("prof").count().orderBy("count",ascending=False)
df3.show()
#india work each profession count
df4 = df1.filter(col("location")=="india").groupby("prof").count().orderBy("count",ascending=False)
df4.show()

