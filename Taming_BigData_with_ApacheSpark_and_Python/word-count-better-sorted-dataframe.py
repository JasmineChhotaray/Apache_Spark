from pyspark.sql import SparkSession
from pyspark.sql import functions as func


spark = SparkSession.builder.appName("WordCount").getOrCreate()

# Read each line of book into a dataframe
inputDF = spark.read.text("E:/Projects/SparkCourse/data/Book.txt")

# As it is unstructured data consisting of rows, we have 1 column name for it by default i.e., 'value'
# dataframe.columnname - inputDF.value
# Split using a regular expression that extract words
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
# filter to make sure that every row contains something and not nothing
words.filter(words.word != '')

# Normalize everything to lowercase
lowercaseWords = words.select(func.lower(words.word).alias("word"))

#  Count up the occurrences of each word
wordCounts = lowercaseWords.groupBy("word").count()

# Sort by counts
wordCountsSorted = wordCounts.sort("count")

# Show the results
wordCountsSorted.show(wordCountsSorted.count())

spark.stop()
