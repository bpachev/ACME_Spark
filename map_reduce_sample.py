from pyspark import SparkContext

sumFile = "nums.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
sumData = sc.textFile(sumFile).cache()

print "Sum of file %d" % sumData.reduce(lambda a,b: int(a)+int(b))
intData = sumData.map(int)
print "Mapped sum of file %d" % intData.reduce(lambda a,b: a+b)
print "Number of nums less than 50 " intData.filter(lambda a: a < 50).count()
