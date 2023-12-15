#Rename a column
#........................................

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
ss=SparkSession.builder.master("local[1]").appName("march").getOrCreate()

lst=[[101,'Vinay','V',26,'Bigdata developer',10000],
     [102,'Anu','P',27,'Python developer',15000],
     [103,'Amal','K',30,'Data Scientist',17500],
     [104,'Arjun','P',29,'AI Researcher',20000],
     [105,'Hari','K',25,'AI Lead',30000],
     [106,'Anugrah','IG',25,'Flutter Developer',50000],
     [107,'Ram','K',25,'ML Engineer',70000]]
print(lst)
col_name = ["Id","fname","lname","age","prof","salary"]
df1=ss.createDataFrame(data=lst,schema=col_name)


df2=df1.withColumn('designation',lit('software_engineer')) \
    .withColumn("Bonus",col("salary")/10) \
    .withColumn("Total_salary",col("salary")+col("Bonus"))\
    .withColumnRenamed("fname","first_name")
df2.show()



