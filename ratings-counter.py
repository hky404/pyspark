from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# creating RDD
lines = sc.textFile("/Users/hkumar2/Downloads/SparkCourse/ml-100k/u.data")

# transforming RDD and putting into a new RDD 'ratings'
ratings = lines.map(lambda x: x.split()[2])

# RDD action
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
