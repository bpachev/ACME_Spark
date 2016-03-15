from pyspark import SparkContext

corpus = "words.txt" 
sc = SparkContext("local", "Simple App")
#Create the RDD
data = sc.textFile(corpus).cache()

print "Num words: " + str(data.count())

def mapper(w):
  return w[0]

def letterFilter(c):
 return c[0] == "e"

print "Words that start with e " + str(data.filter(letterFilter).count())

lens = data.map(len)
print lens.sum()

print data.map(lambda word: (word[0], 1)).reduceByKey(lambda a,b: a + b).collect()
data.flatMap(lambda s: [" "+s[0]]+[s[i:i+2] for i in xrange(len(s)-1)]+[s[-1]+" "]).map(lambda w: (w,1)).reduceByKey(lambda a,b: a+b).saveAsSequenceFile("bigrams.txt")
