from pyspark import SparkConf, SparkContext
import re


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)


def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


lines = sc.textFile("E:/Projects/SparkCourse/data/Book.txt")
words = lines.flatMap(normalizeWords)

# using mapper
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: (x + y))
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if word:
        print(f"{word.decode()} :\t\t {count}")
        # print(word.decode() + ":\t\t" + count)

# for word in results:
#     count = str(word[0])
#     word = word[1].encode('ascii', 'ignore')
#     if word:
#         print(f"{word.decode('ascii')}: \t{count}\n")

