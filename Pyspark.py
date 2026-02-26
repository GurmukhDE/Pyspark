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

Data = [(1, "rob", 24), (2, "bob", 27), (3, "carolina", 30)]
Column = ["id", "name", "age"]

df = spark.createDataFrame(Data, Column)
df.show()

+---+--------+---+
| id|    name|age|
+---+--------+---+
|  1|     rob| 24|
|  2|     bob| 27|
|  3|carolina| 30|
+---+--------+---+

import pyspark.sql.functions as F

df.withColumn("name_upper", F.upper(F.col("name"))).show()

+---+--------+---+----------+
| id|    name|age|name_upper|
+---+--------+---+----------+
|  1|     rob| 24|       ROB|
|  2|     bob| 27|       BOB|
|  3|carolina| 30|  CAROLINA|
+---+--------+---+----------+
