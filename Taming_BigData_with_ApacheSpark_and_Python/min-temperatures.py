from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf=conf)


def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    # Temperature in Fahrenheit
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    # Temperature in degree Celsius
    # temperature = float(fields[3]) * 0.1
    return stationID, entryType, temperature


lines = sc.textFile("E:/Projects/SparkCourse/data/1800.csv")
parsedLines = lines.map(parseLine)
# Min Temperature
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
minTemps = stationTemps.reduceByKey(lambda x, y: min(x, y))
results = minTemps.collect()

# Max Temperature
# maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
# StationTemps = maxTemps.map(lambda x: (x[0], x[2]))
# maxTemps = StationTemps.reduceByKey(lambda x, y: max(x, y))
# results = maxTemps.collect()

for result in results:
    print(f"{result[0]} \t: {result[1]:.2f}F")
    # print(f"{result[0]} \t: {result[1]:.2f}C")
