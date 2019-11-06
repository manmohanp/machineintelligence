from pyspark.sql import functions as F

df = spark.read

from pyspark.sql.types import StructField, StructType, StringType
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

pricePaid = df.option("header", "false").csv(
        "s3://uk-house-data-analysis/data/pp-complete.csv", enforceSchema=True,
        schema=schema)

filterGreaterLondon = pricePaid.filter(pricePaid["county"] == "GREATER LONDON")

from pyspark.sql.functions import date_format, unix_timestamp
formatteddf = filterGreaterLondon.select("district", "county", date_format(
        unix_timestamp("date_of_transfer", "yyyy-MM-dd").cast("timestamp"), "yyyy").alias("dot"))

sorteddf = formatteddf.groupBy("dot", "district").count()

for y in range(1995, 2019):
    yeardf = sorteddf.filter(sorteddf["dot"] == y).sort(sorteddf["count"].desc()).limit(10)
    # this is the costliest operation in the process
    yeardf.write.csv("s3://uk-house-data-analysis/out/yearly/" + str(y))