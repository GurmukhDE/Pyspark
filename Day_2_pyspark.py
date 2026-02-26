import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Stop the old session first
try:
    spark.stop()
except:
    pass

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MyFirstApp") \
    .master("local[*]") \
    .getOrCreate()

print("Spark started:", spark.version)

data = [(80, "maths", "pass"),
       (90, "science", "pass with dictation"),
       (95, "history", """topper in the class or school""")]

col = ["marks", "subject", "remark"]

df = spark.createDataFrame(data, col)

df.show(truncate = False)

df.printSchema()

df.columns

df.describe().show(truncate = False)

marks_below_range = df.filter(df.marks>90)

marks_below_range.show()

df.select("subject").show()

from pyspark.sql.functions import *

df.withColumn("grade", when(df.marks > 90, "A").otherwise("B")
).show(truncate =False)
df.orderBy(df.marks.desc()).show()
