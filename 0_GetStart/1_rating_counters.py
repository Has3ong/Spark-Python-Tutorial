from pyspark import SparkConf, SparkContext
import collections
import os

os.environ["PYSPARK_PYTHON"]="/usr/local/Cellar/python/3.7.4/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/local/Cellar/python/3.7.4/bin/python3"

__BASE_DIR__ = os.path.dirname(os.path.realpath(__file__))

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
conf.set('spark.driver.host','127.0.0.1')
sc = SparkContext(conf = conf)

lines = sc.textFile(__BASE_DIR__ + "/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))

