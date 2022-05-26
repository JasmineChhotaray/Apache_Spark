from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomer")
sc = SparkContext(conf=conf)


def extractCustomerPricePairs(line):
    fields = line.split(',')
    customerID = int(fields[0])
    productPrice = float(fields[2])
    return customerID, productPrice


lines = sc.textFile("E:/Projects/SparkCourse/data/customer-orders.csv")
mappedInput = lines.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)

results = totalByCustomer.collect()
for result in results:
    print(result)