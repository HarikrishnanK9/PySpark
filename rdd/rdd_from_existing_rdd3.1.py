

from pyspark import SparkContext
sc=SparkContext(master='local',appName='march')
rdd=sc.parallelize([i for i in range(1,31)])

rdd1=rdd.map(lambda x:(x,'even') if x%2==0 else (x,'odd'))
rdd1.foreach(print)


#elementum athinte corresponding output um orumich vannal athine parayunnathanu pair rdd
#interview question :Pair rdd
