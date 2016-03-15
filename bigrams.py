from pyspark import SparkContext

bigramPath = "bigrams.txt"  # Should be some file on your system
sc = SparkContext("local", "Simple App")
data = sc.sequenceFile(bigramPath).cache()

sums = data.map(lambda bigram: (bigram[0][0], bigram[1]) ).reduceByKey(lambda a,b: a+b)
print data.map(lambda b: (b[0][0],(b[0][1], b[1]))).join(sums).map(lambda t: )
