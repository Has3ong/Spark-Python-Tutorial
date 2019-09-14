from pyspark import SparkConf, SparkContext
import os

os.environ["PYSPARK_PYTHON"]="/usr/local/Cellar/python/3.7.4/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/local/Cellar/python/3.7.4/bin/python3"

__BASE_DIR__ = os.path.dirname(os.path.realpath(__file__))

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile(__BASE_DIR__ + "/fakefriends.csv")
rdd = lines.map(parseLine)
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
for result in results:
    print(result)
