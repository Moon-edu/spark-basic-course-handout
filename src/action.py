from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, date_format, date_trunc, concat, lit


def build_sparksession() -> SparkSession:
    return SparkSession.Builder() \
        .appName("PySparkExample") \
        .master("local[8]") \
        .getOrCreate()

def run():
    spark = build_sparksession()
    
    df = spark.read.csv("dataset/covid.csv", header=True, schema="date STRING, location STRING, new_cases INT, new_deaths INT, total_cases INT, total_deaths INT")

    # df 결과 모으기 collect
    # result = df.limit(50).collect()
    # for row in result:
    #     print(row)

    # for row in result:
    #     print(row.location) # row["location"]도 가능

    # 첫 번째 결과만 가져오기
    # print(df.first())
    
    # # n 개만 가져오기
    # print(df.take(5))

    # # 갯수
    # print(df.count())

    # # 순회하기
    # df.limit(10).foreach(lambda row: print(row))

    # 파일로 쓰기
    df.limit(100).write.csv("/tmp/covid", header=True)

    spark.stop()

if __name__ == "__main__":
    run()