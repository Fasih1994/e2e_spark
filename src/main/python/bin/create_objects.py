from pyspark.sql import SparkSession
import logging
import logging.config

logging.config.fileConfig(fname="/home/hadoop/e2e_pyspark/src/main/python/utils/logging_to_file.conf")
logger = logging.getLogger(__name__)

def get_spark_object(envn, appName) -> SparkSession:
    if envn == "TEST":
        master = 'local'
    else:
        master = 'yarn'

    logger.info(f"Creating session with {master} for {appName}.")
    spark = SparkSession.builder\
        .master(master)\
        .appName(appName)\
        .getOrCreate()
    logger.info("Session Created Successfully!")
    return spark