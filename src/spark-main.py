from pyspark.sql import SparkSession
from jobs.dep import my_print

def create_session() -> SparkSession:
    spark_conf = {
        "executor": {
            "instances": 3
        }
    }

    return SparkSession.builder \
        .appName("Spark-job-on-cluster-example") \
        .master("spark://master-node:7077") \
        .config("spark.executor.instances", spark_conf["executor"]["instances"]) \
        .config("spark.eventLog.enabled", True) \
        .config("spark.eventLog.dir", "s3a://jobhistory/spark") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://s3-storage:4566") \
        .config("spark.hadoop.fs.s3a.access.key", "some-access-key") \
        .config("spark.hadoop.fs.s3a.secret.key", "some-secret-key") \
        .config("spark.hadoop.fs.s3a.path.style.access", True) \
        .getOrCreate()

def run():
    print("Building session...")
    session = create_session()

    print("Reading data...")
    covid = session.read.csv("s3a://dataset/covid/covid.csv", header=True, schema="date STRING, location STRING, new_cases INT, new_deaths INT, total_cases INT, total_deaths INT")
    who = session.read.csv("s3a://dataset/covid/who-region.csv", header=True, schema="country_full STRING, region STRING, country STRING")

    print("Transforming data...")
    covid.join(who, on=covid["location"] == who["country"]).repartition(3 * 4).foreach(my_print)

    print("Stopping session...")
if __name__ == "__main__":
    run()