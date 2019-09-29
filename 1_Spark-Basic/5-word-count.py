from pyspark import SparkConf, SparkContext
import os

os.environ["PYSPARK_PYTHON"]="/usr/local/Cellar/python/3.7.4/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"]="/usr/local/Cellar/python/3.7.4/bin/python3"

__BASE_DIR__ = os.path.dirname(os.path.realpath(__file__))

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile(__BASE_DIR__ + "/Book.txt")
words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
