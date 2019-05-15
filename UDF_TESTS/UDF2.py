%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col ,udf
from pyspark.sql.types import StructType, ArrayType, StructField, IntegerType, LongType, DecimalType, FloatType ,StringType

from pyspark.sql import SQLContext
my_list=['_Test','workers','strike','pay','rally','free','immigration','_Test','workers','strike','pay','rally','free','immigration','_Test','workers','strike','pay','rally','free','immigration','_Test','workers','strike','pay','rally','free','immigration','_Test','workers','strike','pay','rally','free','immigration','_Test','workers','strike','pay','rally','free','immigration','_Test','workers','strike','pay','rally','free','immigration','_Test','workers','strike','pay','rally','free','immigration','_Test','workers','strike','pay','rally','free','immigration','_Test','workers','strike','pay','rally','free','immigration','_Test','workers','strike','pay','rally','free','immigration','_Test','workers','strike','pay','rally','free','immigration','_Test','workers','strike','pay','rally','free','immigration']

def your_great_code(name,vector):
    result = name
    for value in vector:
        result=result+value

    return result



def process_etl(spark):
    print("drop")
    sqlContext = SQLContext(spark)
    df1 = sqlContext.createDataFrame([("Albeto", 1), ("Albeto", 3), ("ssssss", 2)],
                                     ["name", "id"])

    df2 = sqlContext.createDataFrame([("wwww", 4), ("fds", 5), ("asdasd", 6)],
                                     ["name", "id"])
    df = df1.union(df2)
    df.repartition(1).show()
    broadcast = spark.sparkContext.broadcast(my_list)

    def test_func(name):
        return your_great_code(name,broadcast.value)

    my_udf = udf(test_func, StringType())
    df = df.withColumn('test', my_udf(df['name']))
    df.show()


spark = SparkSession \
    .builder \
    .appName("Python Spark SQL drop") \
    .enableHiveSupport() \
    .getOrCreate()

process_etl(spark)