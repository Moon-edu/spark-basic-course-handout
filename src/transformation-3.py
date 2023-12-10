from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, date_format, date_trunc, concat, lit, last_day, left, max, avg


def build_sparksession() -> SparkSession:
    return SparkSession.Builder() \
        .appName("PySparkExample") \
        .master("local[8]") \
        .getOrCreate()

def run():
    spark = build_sparksession()

    df = spark.read.csv("dataset/covid.csv", header=True, schema="date STRING, location STRING, new_cases INT, new_deaths INT, total_cases INT, total_deaths INT")

    # 하루 최대 확진 환자 수 // select max(new_cases) as 최대하루확진수 from dataset/covid.csv
    # df.agg(max("new_cases").alias("최대하루확진수")).show()

    # # 국가별 최대 확진 환자 수 // select max(new_cases) from dataset/covid.csv group by location
    # df.groupBy("location").agg(max("new_cases")).show()

    # # 국가별 최대 확진 환자 수, 많은 순 정렬 // select max(new_cases) from dataset/covid.csv group by location
    # df.groupBy("location").agg(max("new_cases").alias("max_cases")).orderBy(col("max_cases").desc()).show()

    # # 국가별 최대 확진 환자 수, World는 제외 많은 순 정렬 // select max(new_cases) from dataset/covid.csv group by location
    # df.where(col("location") != "World").groupBy("location").agg(max("new_cases").alias("max_cases")).orderBy(col("max_cases").desc()).show()

    # # 하루 최대 확진 환자 수를 찍은 날짜, 국가명과, 확진 환자 수(국가명 World 제외)
    # select date, location, new_cases from dataset/covid.csv where new_cases = (select max(new_cases) from dataset/covid.csv where location != 'World')
    # # 잘못된 쿼리
    # df.select("date", "location", max("new_cases")).where(col("location") != "World").show()
    # df.select("date", "location", "new_cases").where(col("location") != "World" and col("new_cases") == max("new_cases")).show()
    

    # # 올바른 쿼리
    # max_v = df.where(col("location") != "World").agg(max("new_cases")).collect()[0][0]
    # df.select("date", "location", "new_cases").where(col("new_cases") == max_v).show()

    # who_df = spark.read.csv("dataset/who-region.csv", header=True)
    # # covid데이터와 who-region데이터를 국가명으로 조인하기
    # df.join(who_df, on=df["location"] == who_df["country"]).show()

    # # 데이터 기준일, 국가명, 새 확진자수, 지역(region)을 출력
    # df.join(who_df, on=df["location"] == who_df["country"]).select(df["date"], df["location"], df["new_cases"], who_df["region"]).show()

    # # 아메리카 대륙(Americas) 국가의 데이터만 출력
    # df.join(who_df, on=df["location"] == who_df["country"]).where(who_df["region"] == "Americas").show()

    # # 아메라카 대륙 국가의 데이터를 국가별 최대 확진자수를 출력
    # df.join(who_df, on=df["location"] == who_df["country"]).where(who_df["region"] == "Americas").groupBy(df["location"]).agg(max("new_cases")).show()
    
    # # 실습 과제
    # # 지역별 2020년 3월 평균 확진자 수를 출력하되 region이 International인 경우는 제외
    # df.join(who_df, on=df["location"] == who_df["country"]).where((who_df["region"] != "International") & (left(df["date"], lit(7)) == "2020-03")).groupBy(who_df["region"]).agg(avg(df["new_cases"])).show()

    spark.stop()

if __name__ == "__main__":
    run()