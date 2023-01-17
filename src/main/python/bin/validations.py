from pyspark.sql import SparkSession
import logging
import logging.config

logging.config.fileConfig(fname="/home/hadoop/e2e_pyspark/src/main/python/utils/logging_to_file.conf")
logger = logging.getLogger(__name__)


def validate_spark_object(spark: SparkSession=None):
    try:

        date = spark.sql('select current_date')
        print(f"Validating spark object by current date - {date.collect()}")

    except Exception as e:
        print(f"Error in the method validate_spark_object \n {str(e)}")