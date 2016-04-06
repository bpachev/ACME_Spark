from pyspark import SparkContext
sc = SparkContext("local", "My App")
data = sc.textFile("example.txt")
print data.collect() #Show that data
