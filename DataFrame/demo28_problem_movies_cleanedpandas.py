#Spark Assignment 3 Submitted by Harikrishnan
#MOvies_Cleaned_Pandas
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
ss=SparkSession.builder.master("local[1]").appName("march").getOrCreate()
df=ss.read.csv("/home/harikrishnan/Downloads/movies_cleaned_pandas.csv",sep=',',header=None,inferSchema=True)

df1 = df.withColumnRenamed("_c0","Id").withColumnRenamed("_c1","Name") \
    .withColumnRenamed("_c2","Year").withColumnRenamed("_c3","Rating") \
    .withColumnRenamed("_c4","Duration")
df1.show()

#1. Find row count
print(df1.count())
#2. Remove duplicates and find row count
df2 = df1.drop_duplicates()
print(df2.count())
print("*"*100)
#3. Sort data set by release year in des order
df3 = df1.orderBy("Year",ascending=False)
df3.show()
print("*"*100)
#4. Find rating mxm 5 movies name,year,rating
df4 = df1.orderBy("Rating",ascending=False).select("Name","Year","Rating")
df4.show(5)
print("*"*100)
#5. Find rating minimum 3 movies name,year,rtaing
df5 = df1.orderBy("Rating").select("Name","Year","Rating")
df5.show(3)
print("*"*100)
#6. Find Each year release movie count [count desc order]
df6 = df1.groupby("Year").count().orderBy("count",ascending=False)# ethile count group cheyumbol default aai yearwise countinu count enn heading varunnath aanu
df6.show()
print("*"*100)
#7. Each rating count [count desc order]
df7 = df1.groupby("Rating").count().orderBy("count",ascending=False)
df7.show()
print("*"*100)
#8. 2008 and rating above 3 [collect]
# A. row count
df8 =df1.filter((col("Year")==2008) & (col("Rating")>3))
print(df8.count())
print("*"*100)
#9. Find duration mxm 1 movies name,year,rating,duration
df9 = df1.orderBy("Duration",ascending=False).select("Name","Year","Rating","Duration")
df9.show(1)
print("*"*100)

#10. Find rating mnm 1 movies name,year,rating,duration
df10 = df1.orderBy("Duration").select("Name","Year","Rating","Duration")
df10.show(1)
print("*"*100)
#11. Rating above 4 and relase year above 2005
# A. Rating mxm movies full data
# B. Rating mnm movies full data
df11=df1.filter((col("Rating")>4) & (col("Year")==2005))
df11a = df11.orderBy("Rating",ascending=False)
df11a.show()
df11b = df11.orderBy("Rating")
df11b.show()
print("*"*100)
#12. 2008 movies count
df12 = df1.filter(col("Year")==2008).count()
print(df12)
#13. 1975-2000 movies collect
# A. Row count
print("*"*100)
df13 =df1.filter((col("Year")>=1975) & (col("Year")<=2000)).count()
print(df13)
print("*"*100)
#14. 1975-2000 and rating above 3.5 total row count
df14 = df1.filter((col("Year")<=2000) & (col("Year")>=1975) & (col("Rating")>=3.5)).count()
print(df14)
