import pyspark
from pyspark.sql import SparkSession
ss=SparkSession.Builder.master("local[1]").appName("march").getOrCreate()
