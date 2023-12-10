from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, date_format, date_trunc, concat, lit


def build_sparksession() -> SparkSession:
    return SparkSession.Builder() \
        .appName("PySparkExample") \
        .master("local[8]") \
        .getOrCreate()

def run():
    spark = build_sparksession()

    # # csv 파일 읽기(기본 - ,로 구분된 파일)
    # df = spark.read.csv("dataset/covid.csv", header=True, schema="date STRING, location STRING, new_cases INT, new_deaths INT, total_cases INT, total_deaths INT")
    # df.show()

    # csv 파일 읽기(tab으로 구분된 파일)
    # df = spark.read.csv("dataset/covid.tsv", header=True, sep="\t", schema="date STRING, location STRING, new_cases INT, new_deaths INT, total_cases INT, total_deaths INT")
    # df.show()
    # df.printSchema()

    # # csv 파일 쓰기(,로 구분된 파일)
    # df.limit(100).write.csv("/tmp/covid-default", header=True)

    # # csv 파일 쓰기(|로 구분된 파일)
    # df.limit(100).write.csv("/tmp/covid-pipe", header=True, sep="|")

    # # Write mode
    # # 디렉토리가 존재한다면 기본적으로 에러
    # df.limit(100).write.csv("/tmp/covid-default", header=True)

    # # 디렉토리가 존재한다면 덮어쓰기
    # df.limit(10).write.mode("overwrite").csv("/tmp/covid-default", header=True)

    # # 디렉토리가 존재한다면 덧붙이기
    # df.limit(50).write.mode("append").csv("/tmp/covid-default", header=True)

    # 디렉토리가 존재한다면 무시하기(아무것도 안함)
    # df.limit(5).write.mode("ignore").csv("/tmp/covid-default", header=True)
    
    spark.stop()

if __name__ == "__main__":
    run()