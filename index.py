import pyspark.sql.functions as F
from pyspark.shell import sqlContext
from pyspark.sql.types import *
from pyspark.sql import SparkSession

# Creation of Spark Session object and initial Dataframe
spark = SparkSession.builder.getOrCreate()
df = spark.read.format("Json").option("inferSchema", "true").load("recipes.json")

# UDF to convert time strings to minutes
def toMin(stg):
    min = 0
    x = stg.replace("PT", "")
    if x == "":
        return min
    else:
        x1 = x.replace("H", " H ")
        x2 = x1.replace("M", " M ")
        y = x2.split()
        if (len(y) == 4):
            min += int(y[0]) * 60 + int(y[2])
            return min
        elif (len(y) == 2 and y.count('H') == 1):
            min += int(y[0]) * 60
            return min
        else:
            min += int(y[0])
            return min

# Regestering the UDF
strToMin_UDF = F.udf(lambda z: toMin(z), IntegerType())

# Transformation to perform data cleaning
df1 = df.withColumn("cookTime", strToMin_UDF("cookTime")).withColumn("prepTime", strToMin_UDF("prepTime"))

# UDFs to add cook and prep times of a dish and add a difficulty level label to it
dlevel_udf = F.udf(lambda x,y : "Easy" if (x+y)<30 else "Medium" if (x+y)<60 else "Difficult", StringType())
add_udf = F.udf(lambda x,y : x+y, IntegerType())

# Transformation to add difficultyLevel and TotalTime Columns
df2 = df1.withColumn("TotalTime",add_udf("cookTime", "prepTime")).withColumn("difficultyLevel", dlevel_udf("cookTime", "prepTime"))

# Average calculation for each Level
df2.groupBy("difficultyLevel").agg(F.mean("TotalTime")).write.parquet("/output/result.parquet")

