from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, date_format, date_trunc, concat, lit


def build_sparksession() -> SparkSession:
    return SparkSession.Builder() \
        .appName("PySparkExample") \
        .master("local[8]") \
        .getOrCreate()

def run():
    spark = build_sparksession()

    # 여러 줄로 구성된 json 파일 읽기
    # df = spark.read.json("dataset/covid-multiline.json", schema="date STRING, location STRING, new_cases INT, new_deaths INT, total_cases INT, total_deaths INT")
    # df.show()

    # # 배열로 구성된 json 파일 읽기
    # df = spark.read.json("dataset/covid-array.json", schema="date STRING, location STRING, new_cases INT, new_deaths INT, total_cases INT, total_deaths INT")
    # df.show()

    # # json 파일 쓰기
    # df.limit(100).write.json("/tmp/covid-default")
    # spark.stop()

if __name__ == "__main__":
    run()