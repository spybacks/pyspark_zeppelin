%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col ,udf
from pyspark.sql.types import StructType, ArrayType, StructField, IntegerType, LongType, DecimalType, FloatType ,StringType

from pyspark.sql import SQLContext
my_list=['_Test','workers','strike','pay','rally','free','immigration']


def test_func(vector,name):
    return name + vector[0]


def run_udf_wrapper(my_list):
    return udf(lambda name: test_func(my_list,name))


def process_etl(spark):
    print("drop")
    sqlContext = SQLContext(spark)
    df1 = sqlContext.createDataFrame([("Albeto", 1), ("Albeto", 3), ("ssssss", 2)],
                                     ["name", "id"])

    df2 = sqlContext.createDataFrame([("wwww", 4), ("fds", 5), ("asdasd", 6)],
                                     ["name", "id"])
    df = df1.union(df2)
    df.repartition(1).show()
    df = df.withColumn("test",run_udf_wrapper(my_list)(col('name')))
    df.show()


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL drop") \
    .enableHiveSupport() \
    .getOrCreate()

process_etl(spark)