#student id,name,subj1 mark,subj2 mark,subj 3 mark

import pyspark
from pyspark.sql import SparkSession
ss=SparkSession.builder.master("local[1]").appName("march").getOrCreate()

lst=[[101,"Harikrishnan",100,100,100],
     [102,"Harinarayanan",95,98,97],
     [103,"Raghupathi",96,94,90],
     [104,"Amala",90,88,78],
     [105,"Ramya",100,98,98]]

print(lst)

col_name=["Id","Name","Subj1","Subj2","Subj3"]
df1=ss.createDataFrame(data=lst,schema=col_name)
df1.show(3)
