import asyncio

from pyspark.sql import SparkSession
from pyspark.sql import types as T

spark = SparkSession.builder.getOrCreate()


async def df1():
    df = spark.createDataFrame(
        [
            (1, "foo"),  # Add your data here
            (2, "bar"),
        ],
        T.StructType(  # Define the whole schema within a StructType
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("label", T.StringType(), True),
            ]
        ),
    )

    return df


async def df2():
    df = spark.createDataFrame(
        [
            (3, "fok"),  # Add your data here
            (4, "lol"),
        ],
        T.StructType(  # Define the whole schema within a StructType
            [
                T.StructField("id", T.IntegerType(), True),
                T.StructField("camel", T.StringType(), True),
            ]
        ),
    )

    return df


async def main():
    results = await asyncio.gather(df1(), df2())
    print(results)


asyncio.run(main())
