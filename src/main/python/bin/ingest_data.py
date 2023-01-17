from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame

import logging
import logging.config as log_conf

log_conf.fileConfig(fname="/home/hadoop/e2e_pyspark/src/main/python/utils/logging_to_file.conf")
logger = logging.getLogger(__name__)

def load_files(spark:SparkSession = None, file_dir: str=None, file_format: str=None, header: str='True', infer_schema: str='True')->DataFrame:
    try:
        logger.info(f'Loading data {file_dir} ')
        if file_format == 'parquet':
            df = spark.read.format(file_format).load(file_dir)
        elif file_format == 'csv':
            df = spark.read.format(file_format).options(header=header).options(inferSchema=infer_schema).load(file_dir)

    except Exception as e:
        logger.error(msg="",exc_info=True)
    else:
        logger.info(f"File {file_dir} is loaded successfully!")
    return df