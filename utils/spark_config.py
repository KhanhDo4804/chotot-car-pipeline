from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def get_spark_session(app_name="Used_Car_Lakehouse"):
    
    # Use jars placed in the container at /opt/airflow/jars
    delta_jar = "/opt/airflow/jars/delta-spark_2.12-3.1.0.jar"
    postgresql_jar = "/opt/airflow/jars/postgresql-42.7.3.jar"

    jars = ",".join([delta_jar, postgresql_jar])

    builder = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "8g")
        .config("spark.memory.fraction", "0.6")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.jars", jars)
        .config("spark.driver.userClassPathFirst", "true")
        .config("spark.executor.userClassPathFirst", "true")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0,org.postgresql:postgresql:42.7.2") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \

        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    return spark