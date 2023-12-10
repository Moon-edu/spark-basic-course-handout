from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, date_format, date_trunc, concat, lit, last_day, left


def build_sparksession() -> SparkSession:
    return SparkSession.Builder() \
        .appName("PySparkExample") \
        .master("local[8]") \
        .getOrCreate()

def run():
    spark = build_sparksession()

    df = spark.read.csv("dataset/covid.csv", header=True, schema="date STRING, location STRING, new_cases INT, new_deaths INT, total_cases INT, total_deaths INT")

    # new_cases가 null인 경우만 출력
    # df.filter(col("new_cases").isNull()).show()

    # 주의 null 비교는 == None(null)으로 하면 안됨, null == null은 null(true도 false도 아님)
    # df.filter(col("new_cases") == None).show() 
    # df.select((lit(None) == lit(None)).alias("is_same")).show()
    # df.select((lit(1) == lit(1)).alias("is_same")).show()

    # total_cases가 10이상이고 location이 World가 아닌 경우만 출력 (& |을 위해서 괄호를 꼭 써줘야 함)
    df.where((col("total_cases") >= 10) & (col("location") != "World")).show()

    # # 날짜가 2020-03-11 ~ 2020-03-13 사이인 경우만 출력(between 조건)
    # df.where(col("date").between("2020-03-11", "2020-03-13")).show()

    # # new_death가 1인 경우
    # df.where(col("new_deaths") == 1).show()

    # # 특정 국가만 출력(in 조건)
    # df.where(col("location").isin("South Korea", "Japan", "China")).show()

    # # 특정 국가만 제외하고 출력(not in 조건) 국가명 내림차순(역순) 정렬
    # df.where(~col("location").isin("South Korea", "Japan", "China")).orderBy(col("location").desc()).show()

    # # select와 결합
    # df.where(col("new_deaths") == 1).select("date", "location", "new_deaths", "total_cases").show()

    # # where절에 함수 적용, 2020년 3월 데이터만 출력(1~31일)
    # df.where(date_trunc("month", to_date(col("date"), "yyyy-MM-dd")) == "2020-03-01").show()
    # 더 간단한 방법(왼쪽부터 7자리만 비교)
    # df.where(left("date", lit(7)) == "2020-03").show()

    # # 실습 과제
    # # 각 월의 마지막 날짜의 데이터만 뽑아오되, 날짜(date), 국가(location), 총 확진자(total_cases)만 출력

    # df.where(to_date(col("date")) == last_day(to_date(col("date"), "yyyy-MM-dd"))).select("date", "location", "total_cases").show()

    spark.stop()

if __name__ == "__main__":
    run()