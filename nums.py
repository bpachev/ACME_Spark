from pyspark import SparkContext
from math import log

sc = SparkContext("local", "My App")
data = sc.textFile("nums.txt")
print "Num Lines: %d" % data.count()
print "First five elements.", data.take(5)
print "Maximum Element " + data.reduce(max)
print "Total dataset ", data.collect()

intData = data.map(int)
print "Numbers less than 50: %d" % intData.filter(lambda a: a < 50).count()
print "Sum %d " % intData.reduce(lambda a,b : a+b)
print "Sum Of Squares %d" % intData.map(lambda x: x*x).reduce(lambda a,b: a+b)
#print sumData.collect()
#print "Sum of file %d" % sumData.reduce(lambda a,b: int(a)+int(b))
#intData = sumData.map(int)
#print "Mapped sum of file %d" % intData.reduce(lambda a,b: a+b)
#print "Number of nums less than 50 %d" % intData.filter(lambda a: a < 50).count()
