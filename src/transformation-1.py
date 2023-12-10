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
    # df.show()

    # 특정 칼럼 선택 select date, location, total_cases from dataset/covid.csv
    # df.select("date","location","total_cases").show()

    # 칼럼명 변경 select date as 기준일, location as 국가, total_cases as 발병건수 from dataset/covid.csv
    # df.select(col("date").alias("기준일"),col("location").alias("국가"), col("total_cases").alias("발병건수")).show()

    # Null 대신 0으로 채우기
    # select case when new_cases is null then 0 else new_cases end as new_cases, case when new_deaths is null then 0 else new_deaths end as new_deaths, case when total_cases is null then 0 else total_cases end as total_cases, case when total_deaths is null then 0 else total_deaths end as total_deaths from dataset/covid.csv
    # df.select("date",
    #         "location",
    #         when(col("new_cases").isNull(), 0).otherwise(col("new_cases")).alias("new_cases"), 
    #         when(col("new_deaths").isNull(), 0).otherwise(col("new_deaths")).alias("new_deaths"), 
    #         when(col("total_cases").isNull(), 0).otherwise(col("total_cases")).alias("total_cases"),
    #         when(col("new_deaths").isNull(), 0).otherwise(col("total_deaths")).alias("total_deaths"),
    # ).show()
    # 더 간단한 null채우기
    # df.fillna(0).show()
    # df.fillna(0, subset=["new_cases", "new_deaths"]).show()
    
    # 칼럼 추가 select *, new_cases + new_deaths + total_cases + total_deaths as sum_4cols from dataset/covid.csv
    # df.select("*").fillna(0).withColumn("sum_4cols", col("new_cases") + col("new_deaths") + col("total_cases") + col("total_deaths")).show()

    # 칼럼 삭제 select date, location, new_death, total_cases, total_death from dataset/covid.csv
    # df.drop("new_cases").show()

    # 칼럼 타입 변경 select date, location, new_cases, new_deaths, total_cases, total_deaths, case(new_cases as string) from dataset/covid.csv
    # df.printSchema()
    # new_df = df.withColumn("new_cases", col("new_cases").cast("STRING"))
    # new_df.printSchema()

    # 날짜 타입으로 변경 select date, location, new_cases, new_deaths, total_cases, total_deaths, to_date(new_cases, 'yyyy-MM-dd') from dataset/covid.csv
    # df.printSchema()
    # new_df = df.withColumn("date", to_date(col("new_cases"), "yyyy-MM-dd"))
    # new_df.printSchema()

    # 다른 날짜 형태로 바꾸기(yyyy-MM-dd -> yyyyMMdd) select date, location, new_cases, new_deaths, total_cases, total_deaths, date_format(to_date(new_cases, 'yyyy-MM-dd'), 'yyyyMMdd') from dataset/covid.csv
    # df.printSchema()
    # new_df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    # new_df.withColumn("date", date_format(col("date"), "yyyyMMdd")).show()
    
    # 매월 1일로 맞추기
    # new_df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    # new_df.withColumn("date", date_trunc("month", col("date"))).show()

    # Tip: 정말 많이 쓰는 분기별로 묶기 위한 사전 작업, 분기단위로 날짜 맞추기
    # new_df = df.withColumn("date", to_date(col("date"), "yyyy-MM-dd"))
    # new_df.withColumn("date_q", date_trunc("quarter", col("date"))).show()
    
    # 정렬
    # df.orderBy(col("location").desc()).show()

    # 중복 제거
    # df.select("location").distinct().orderBy(col("location").desc()).show()

    # 문자열 연산, 숫자 칼럼을 모아 리포트 칼럼 만들기
    # df.select(concat(col("location"), lit("에서는 "), col("date"), lit("에 "), col("new_cases"), lit("건 발생함")).alias("report")).show(truncate=False)

    # 좀 더 복잡한 조건적 리포트 칼럼 만들기: 0건이면 "어느 나라에서 어느날짜에 발생하지 않음", 그렇지 않으면 어느나라에서 어느 날짜에 몇 건 발생한지 리포트 문자열 만들기
    # df.fillna(0).select(when(col("new_cases") == 0, concat(col("location"), lit("에서는 "), col("date"), lit("에 "), lit("발생하지 않음")))
    #         .otherwise(concat(col("location"), lit("에서는 "), col("date"), lit("에 "), col("new_cases"), lit("건 발생함")))
    #         .alias("report")).show(truncate=False)
    
    # 실습 과제
    # 위의 리포트를 개선하고자 합니다. 최초 데이터(df)에서 
    # new_cases가 null인 경우는 리포트 내용이: 데이터가 없음 
    # 0인 경우는: "감염사례 없음", 0보다 큰 경우는: "새로운 {new_cases}건이 보고됨"으로 하는 
    # new_cases_report column을 만들어 stdout 출력하세요. 출력결과는 특정 길이로 짤리면 안됩니다.
    # 주의:  df.fillna를 이용하지 않고 df를 이용하셔야합니다

    # df.select(
    #     when(col("new_cases").isNull(), lit("데이터가 없음"))
    #     .when(col("new_cases") == 0, lit("감염사례 없음"))
    #     .otherwise(concat(lit("새로운 "), col("new_cases"), lit("건이 보고됨")))
    #     .alias("new_cases_report")).show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    run()