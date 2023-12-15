
# 1. Find row count
# 2. Remove duplicates and find row count
# 3. Sort data set by release year in des order
# 4. Find rating mxm 5 movies name,year,rating
# 5. Find rating minimum 3 movies name,year,rtaing
# 6. Find Each year release movie count [count desc order]
# 7. Each rating count [count desc order]
# 8. 2008 and rating above 3 [collect]
# A. row count
# 9. Find duration mxm 1 movies name,year,rating,duration
# 10. Find rating mnm 1 movies name,year,rating,duration
# 11. Rating above 4 and relase year above 2005
# A. Rating mxm movies full data
# B. Rating mnm movies full data
# 12. 2008 movies count
# 13. 1975-2000 movies collect
# A. Row count
# 14. 1975-2000 and rating above 3.5 total row count



from pyspark import SparkContext
sc = SparkContext(master='local',appName='march').getOrCreate()
rdd = sc.textFile("/home/harikrishnan/Downloads/movies_cleaned_pandas.csv")
# rdd.foreach(print)

rdd1 = rdd.map(lambda x: x.split(','))
row_count = rdd1.count()
print(row_count)
print("1"*100)

rdd2 = rdd.distinct()
row_count2 = rdd2.count()
print(row_count2)
print("2"*100)

rdd3=rdd2.map(lambda x:x.split(','))

rdd4=rdd3.sortBy(lambda x:x[2],ascending=False)
rdd4.foreach(print)
print("3"*100)

rdd5=rdd3.sortBy(lambda x:x[3],ascending=False)
rdd6=rdd5.map(lambda x:(x[1],x[2],x[3]))
print(rdd6.take(5))
print("4"*100)

rdd7= rdd3.sortBy(lambda x:x[3],ascending=True)
rdd8= rdd7.map(lambda x:(x[1],x[2],x[3]))
print(rdd8.take(3))
print("5"*100)

rdd9 = rdd3.map(lambda x: (x[2],1))
rdd10 = rdd9.reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False)
rdd10.foreach(print)
print("6"*100)

rdd11=rdd3.map(lambda x:(x[3],1))
rdd12=rdd11.reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending=False)
rdd12.foreach(print)
print("7"*100)

rdd13=rdd3.filter(lambda x:x[2]=="2008" and x[3]>="3")
rdd13.foreach(print)
print(rdd13.count())
print("8"*100)

rdd14 = rdd3.sortBy(lambda x:x[4],ascending=False).map(lambda x:(x[1],x[2],x[3],x[4])).first()
print(rdd14)
print("9"*100)

rdd15=rdd3.sortBy(lambda x:x[3],ascending=True).map(lambda x:(x[1],x[2],x[3],x[4])).first()
print(rdd15)
print("10"*100)

rdd16=rdd3.filter(lambda x:x[3]>"4" and x[2]>"2005")
rdd17=rdd16.sortBy(lambda x:x[3],ascending=False).take(5)
print(rdd17)
rdd18=rdd16.sortBy(lambda x:x[3],ascending=True).take(5)
print(rdd18)
print("11"*100)

rdd19=rdd3.filter(lambda x:x[2]=="2008")
print(rdd19.count())
print("12-"*100)

rdd20=rdd3.filter(lambda x:"1975"<=x[2]<="2000")
print(rdd20.count())
print("13-"*100)

rdd21=rdd3.filter(lambda x:"1975"<=x[2]<"2000" and x[3]>"3.5")
print(rdd21.count())
print("14-"*100)
