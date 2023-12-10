from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, date_format, date_trunc, concat, lit


def build_sparksession() -> SparkSession:
    return SparkSession.Builder() \
        .appName("PySparkExample") \
        .master("local[8]") \
        .getOrCreate()

def run():
    spark = build_sparksession()

    df1 = spark.read.csv("dataset/question-sample.csv", header=True)
    df2 = df1.where(col("length") > 10)
    df3 = df2.where(col("sentence").contains("What"))
    df4 = df2.where(col("sentence").contains("How"))
    """
    question-sample.csv ___ df1 ___ df2 ___ df3
                                       |
                                       \___ df4
    """

    print(df3)
    print(df4)

    # for r in df3:
    #     print(r)
    # for r in df4:
    #     print(r)


    # df3.show()
    spark.stop()

if __name__ == "__main__":
    run()