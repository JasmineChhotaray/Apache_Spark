from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("TotalSpentByCustomer").master("local[*]").getOrCreate()

# Create schema when reading customer-orders
customerOrderSchema = StructType([
    StructField("cust_id", IntegerType(), True),
    StructField("item_id", IntegerType(), True),
    StructField("amount_spent", FloatType(), True)])

# Load up the data into spark dataset
customersDF = spark.read.schema(customerOrderSchema).csv("E:/Projects/SparkCourse/data/customer-orders.csv")

totalByCustomer = customersDF.groupBy("cust_id").agg(func.round(func.sum("amount_spent"), 2).alias("total_spent"))

totalByCustomerSorted = totalByCustomer.sort("total_spent")

# to force show the entire dataframe, we pass it to the show()
totalByCustomerSorted.show(totalByCustomerSorted.count())

# Stop session
spark.stop()