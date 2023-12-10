from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, date_format, date_trunc, concat, lit
import psycopg
import os, lzma
import time

def build_sparksession() -> SparkSession:
    return SparkSession.Builder() \
        .appName("PySparkExample") \
        .master("local[8]") \
        .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0") \
        .getOrCreate()

def batch_insert():
    """
    한번에 insert
    """
    data = []
    with open("dataset/questions.txt", "r") as f:
        for l in f:
            data.append((l, ))
    with psycopg.connect(host="localhost", port=5432, dbname="postgres", user="postgres", password="password") as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO log_table(sentence) values(%s)", data)

def chunk_insert():
    """
    10000개씩 끊어서 insert
    """
    data = []
    with open("dataset/questions.txt", "r") as f:
        for l in f:
            data.append((l, ))
        if len(data) == 10000:
            __batch_insert(data)
            data = []
    if len(data) > 0:
        __batch_insert(data)

def __batch_insert(data: list):
    with psycopg.connect(host="localhost", port=5432, dbname="postgres", user="postgres", password="password") as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.executemany("INSERT INTO log_table(sentence) values(%s)", data)

def parallel_insert():
    spark = build_sparksession()
    df = spark.read.text("dataset/questions.txt")
    df.write.mode("overwrite").jdbc(url="jdbc:postgresql://localhost:5432/postgres", table="log_table", properties={"user":"postgres", "password":"password", "driver":"org.postgresql.Driver"})

def perf_test():
    if not os.path.exists("dataset/questions.txt"):
        dest_file = "dataset/questions.txt"
        zip_file_path = os.path.abspath(f"{dest_file}.xz")
        extract_to = os.path.abspath(dest_file)
        with lzma.open(zip_file_path, "rb") as r, open(extract_to, "wb") as w:
            w.write(r.read())

    start = time.time()
    batch_insert()
    # chunk_insert()
    # parallel_insert()
    end = time.time()
    print(f"Elapsed Time: {end - start}")

def run():
    # spark = build_sparksession()

    #  localhost:5432 postgre에서 covid 데이터 가져오기, table은 covid
    # df = spark.read.jdbc(url="jdbc:postgresql://localhost:5432/postgres", table="covid", properties={"user":"postgres", "password":"password", "driver":"org.postgresql.Driver"})
    # df.show()
    
    # # 테이블에 데이터 넣기(테이블 없을 경우 만들어짐)
    # df.limit(100).write.jdbc(url="jdbc:postgresql://localhost:5432/postgres", table="covid2", properties={"user":"postgres", "password":"password", "driver":"org.postgresql.Driver"})

    # # 테이블에 데이터 넣기 존재하면 에러
    # df.limit(100).write.jdbc(url="jdbc:postgresql://localhost:5432/postgres", table="covid2", properties={"user":"postgres", "password":"password", "driver":"org.postgresql.Driver"})

    # # 테이블에 데이터 넣기 존재하면 덮어쓰기
    # df.limit(10).write.mode("overwrite").jdbc(url="jdbc:postgresql://localhost:5432/postgres", table="covid2", properties={"user":"postgres", "password":"password", "driver":"org.postgresql.Driver"})

    # 테이블에 데이터 넣기 존재하면 덧붙이기
    # df.limit(50).write.mode("append").jdbc(url="jdbc:postgresql://localhost:5432/postgres", table="covid2", properties={"user":"postgres", "password":"password", "driver":"org.postgresql.Driver"})
    perf_test()
if __name__ == "__main__":
    run()