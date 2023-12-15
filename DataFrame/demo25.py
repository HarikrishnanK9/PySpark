#Evaluating function 1:COUNT()
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
ss=SparkSession.builder.master("local[1]").appName("march").getOrCreate()
df=ss.read.csv("/home/harikrishnan/Downloads/customer",sep=',',header=False,inferSchema=True)
# df.show()
df1=df.withColumnRenamed("_c0","Id").withColumnRenamed("_c1","fname") \
       .withColumnRenamed("_c2","lname").withColumnRenamed("_c3","age") \
       .withColumnRenamed("_c4","prof").withColumnRenamed("_c5","location")
df1.printSchema()

#Oro professionilum work cheyunnavarude maximum age
df2=df1.groupby("prof").max("age")
df2.show()

