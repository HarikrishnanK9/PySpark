import pyspark
from pyspark.sql import SparkSession
ss=SparkSession.builder.master("local[1]").appName("march").getOrCreate()

lst=[[101,'Vinay','V',26,'Bigdata developer',10000],
     [102,'Anu','P',27,'Python developer',15000],
     [103,'Amal','K',30,'Data Scientist',17500],
     [104,'Arjun','P',29,'AI Researcher',20000],
     [105,'Hari','K',25,'AI Lead',30000],
     [106,'Anugrah','IG',25,'Flutter Developer',50000],
     [107,'Ram','K',25,'ML Engineer',70000]]
print(lst)
