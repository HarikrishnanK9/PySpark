import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
ss=SparkSession.builder.master("local[1]").appName("march").getOrCreate()

df1=ss.read.csv("/home/harikrishnan/Downloads/Hive_Joining/student",sep=',',header=None,inferSchema=True)
df2=ss.read.csv("/home/harikrishnan/Downloads/Hive_Joining/results",sep=',',header=None,inferSchema=True)


df3=df1.withColumnRenamed("_c0","Name").withColumnRenamed("_c1","RollNo1")
df4=df2.withColumnRenamed("_c0","RollNo2").withColumnRenamed("_c1","Result")

df3.show()
df4.show()
print("*"*100)
df5=df3.join(df4,df3.RollNo1==df4.RollNo2,'inner').filter(col("Result")=="pass").select("RollNo1","Name","Result") #chila filil same Peru kodukkan aavilla like Id in previous case
df5.show()
