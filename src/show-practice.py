from pyspark.sql import SparkSession


def build_sparksession() -> SparkSession:
    return SparkSession.Builder() \
        .appName("PySparkExample") \
        .master("local[8]") \
        .getOrCreate()

def run():
    spark = build_sparksession()

    df = spark.read.csv("dataset/question-sample.csv", header=True)
    df.show()
    # df.show(n=3)
    # df.show(truncate=False)
    # df.show(vertical=True)
    
    spark.stop()

if __name__ == "__main__":
    run()