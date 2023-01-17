from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
import logging
import logging.config

logging.config.fileConfig(fname="/home/hadoop/e2e_pyspark/src/main/python/utils/logging_to_file.conf")
logger = logging.getLogger(__name__)


def validate_spark_object(spark: SparkSession=None):
    try:

        date = spark.sql('select current_date')
        logger.info(f"Validating spark object by current date - {date.collect()}")

    except Exception as e:
        logger.error(f"Error in the method validate_spark_object \n {str(e)}", exc_info=True)


def df_count(df: DataFrame=None, df_name:str = None):
    try:
        logger.info(f"Counting for {df_name}.")
        count = df.count()
        logger.info(f"The total rows are {count}")
    except Exception as e:
        logger.error(f"some error has occured {str(e)}", exc_info=True)
    else: 
        logger.info(f"validation completed by df_count for {df_name}")


def df_top10_rec(df: DataFrame=None, df_name:str = None):
    try:
        logger.info(f"Printing top 10 records for {df_name}.")
        df_pandas = df.limit(10).toPandas()
        logger.info(f"\n\t {df_pandas.to_string(index=False)}")
    
    except Exception as e:
        logger.error(f"some error has occured {str(e)}", exc_info=True)
    else: 
        logger.info(f"validation completed by df_top10_rec for {df_name}")