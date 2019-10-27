from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, unix_timestamp
import datetime
import uuid

from pyspark.sql.types import StructField, StructType, StringType, IntegerType


def log(msg):
    print(str(datetime.datetime.now()) + " - " + msg)

session = SparkSession \
    .builder \
    .appName("TopLondonTransactions") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

df = session.read
schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("price", StringType(), True),
    StructField("date_of_transfer", StringType(), True),
    StructField("postcode", StringType(), True),
    StructField("property_type", StringType(), True),
    StructField("old_new", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("paon", StringType(), True),
    StructField("saon", StringType(), True),
    StructField("street", StringType(), True),
    StructField("locality", StringType(), True),
    StructField("town_city", StringType(), True),
    StructField("district", StringType(), True),
    StructField("county", StringType(), True),
    StructField("ppd_category_type", StringType(), True),
    StructField("record_status", StringType(), True)
])

log("Loading file...")

pricePaid = df.option("header", "false").csv("/Users/manamohanpanda/Downloads/pp-complete.csv", enforceSchema=True, schema=schema)

# filter greater london data
filterGreaterLondon = pricePaid.filter(pricePaid["county"] == "GREATER LONDON")
# filter district and county and their sale date as well as transform date in MM/yyyy format
# formatteddf = filterGreaterLondon.select("district","county",date_format(unix_timestamp("date_of_transfer", "yyyy-MM-dd").cast("timestamp"),"MM/yyyy").alias("dot"))

log("---1--")
# group data by date of transfer and district, count each district sale
sorteddf = filterGreaterLondon.groupBy("district").count()

sorteddf.show(truncate = False)
log("--2---")

# sorteddf.write.csv("out/"+uuid.uuid1().hex)

session.stop()

log("exiting...")