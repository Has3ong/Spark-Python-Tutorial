import re
from pyspark import SparkConf, SparkContext
import os

os.environ["PYSPARK_PYTHON"]="/usr/local/Cellar/python/3.7.4/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/local/Cellar/python/3.7.4/bin/python3"

__BASE_DIR__ = os.path.dirname(os.path.realpath(__file__))

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile(__BASE_DIR__ + "/Book.txt")

words = input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)
