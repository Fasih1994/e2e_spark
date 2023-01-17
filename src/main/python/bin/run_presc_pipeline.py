## IMport everything
import sys
import get_all_variables as var
from create_object import get_spark_object
from validations import validate_spark_object
import logging
import logging.config as log_conf

log_conf.fileConfig(fname="/home/hadoop/e2e_pyspark/src/main/python/utils/logging_to_file.conf")
logger = logging.getLogger('root')
def main():
    try:
        logger.info("Sarting spark code")
        spark = get_spark_object(envn='YARN',appName=var.appName)
        logger.info("Spark object created!")
        validate_spark_object(spark=spark)


    except Exception as e:
        logger.error(f"Error in main(): \n{str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
