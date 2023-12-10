from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, to_date, date_format, date_trunc, concat, lit, last_day, left, max, avg


def build_sparksession() -> SparkSession:
    return SparkSession.Builder() \
        .appName("PySparkExample") \
        .master("local[8]") \
        .getOrCreate()

def run():
    spark = build_sparksession()

    # spark.sql("""
    #     CREATE TEMPORARY VIEW covid
    #     USING csv 
    #     OPTIONS (
    #         path 'dataset/covid.csv',
    #         header true
    #     )
    # """)
    # spark.sql("""
    #     CREATE TEMPORARY VIEW who
    #     USING csv 
    #     OPTIONS (
    #         path 'dataset/who-region.csv',
    #         header true
    #     )
    # """)

    # spark.sql("select date, location, total_cases from covid").show()

    # spark.sql("select max(total_cases) from covid where location != 'World'").show()

    # spark.sql("select region, sum(new_cases) from covid c join who w on c.location = w.country group by region").show()


    spark.stop()

if __name__ == "__main__":
    run()