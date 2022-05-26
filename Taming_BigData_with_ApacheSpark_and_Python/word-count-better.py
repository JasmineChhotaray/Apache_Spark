from pyspark import SparkConf, SparkContext
import re


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


lines = sc.textFile("E:/Projects/SparkCourse/data/Book.txt")
# words = lines.flatMap(lambda x: x.split())
words = lines.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if cleanWord:
        print(cleanWord, count)