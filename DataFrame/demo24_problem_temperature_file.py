import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
ss=SparkSession.builder.master("local[1]").appName("march").getOrCreate()
df=ss.read.csv("/home/harikrishnan/Downloads/Temperature",sep=' ',header=None,inferSchema=True)
df1=df.withColumnRenamed("_c0","Year").withColumnRenamed("_c1","Temperature")
df1.show()

df2 = df1.groupBy("Year").max("Temperature")
df2.show()
 #df.printSchema() kodukkuka string aanenkil aavilla
