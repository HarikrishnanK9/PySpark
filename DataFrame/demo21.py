
import pyspark
from pyspark.sql import SparkSession
ss=SparkSession.builder.master("local[1]").appName("march").getOrCreate()
df=ss.read.csv("/home/harikrishnan/Downloads/sample4.txt",sep=',',header=None)
df1=df.withColumnRenamed('_c0','Id').withColumnRenamed('_c1','fname')\
      .withColumnRenamed('_c2','lname').withColumnRenamed('_c3','age') \
      .withColumnRenamed('_c4','phno').withColumnRenamed('_c5','location')
df1.show()

#newdataframe = olddataframe.groupby('colname').count()

df2 = df1.groupby("location").count().orderBy("count",ascending=False) #evide default aai order cheyum peru kodukkanda
df2.show()
