%pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, col ,udf
from pyspark.sql.types import StructType, ArrayType, StructField, IntegerType, LongType, DecimalType, FloatType ,StringType
from pyspark.sql.functions import broadcast

from pyspark.sql import SQLContext


def process_etl(spark):
    print("drop")
    sqlContext = SQLContext(spark)
    df1 = sqlContext.createDataFrame([("aaaa", 1), ("bbbb", 3), ("ssssss", 2)],
                                     ["name", "id"])

    df2 = sqlContext.createDataFrame([("qweqweq", 1), ("fdqweqwes", 2), ("aqweqwesdasd", 3)],
                                     ["newname", "id"])
    df = df1.join(broadcast(df2),"id")
    df.repartition(1).show()



spark = SparkSession \
    .builder \
    .appName("Python Spark SQL drop") \
    .enableHiveSupport() \
    .getOrCreate()

process_etl(spark)