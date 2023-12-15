from pyspark import SparkContext
sc=SparkContext(master='local',appName='march').getOrCreate()
rdd=sc.textFile('/home/harikrishnan/PycharmProjects/spark_march/sample')
rdd.foreach(print)

print("*"*100)

rdd1 = rdd.map(lambda x:"yes" if "a" in x else "no") #oro lininum corresponding aai letter a varunn untenil yes allenkil no enn print cheyum
rdd1.foreach(print)

