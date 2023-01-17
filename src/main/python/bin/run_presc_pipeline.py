## IMport everything
import sys
import logging
import logging.config as log_conf

import get_all_variables as var
from create_objects import get_spark_object
from validations import validate_spark_object, df_count, df_top10_rec
from ingest_data import load_files
from preprocess import clean_data

log_conf.fileConfig(fname="/home/hadoop/e2e_pyspark/src/main/python/utils/logging_to_file.conf")
logger = logging.getLogger('root')
def main():
    try:
        logger.info("Sarting spark code")
        spark = get_spark_object(envn='YARN',appName=var.appName)
        logger.info("Spark object created!")
        validate_spark_object(spark=spark)

        # Load city dimensions
        dim_city = load_files(
            spark=spark, file_dir=var.stagging_dim_city, file_format='parquet')
        df_count(df=dim_city, df_name='dim_city')
        
        # Load city dimensions
        fact_city = load_files(
            spark=spark, file_dir=var.stagging_fact, 
            file_format='csv',infer_schema=var.inferSchema,
            header=var.header)
        df_count(df=fact_city,df_name="fact_city")

        dim_city = clean_data(df=dim_city,df_name='dim_city')
        df_top10_rec(df=dim_city, df_name='dim_city')
        
        fact_city = clean_data(df=fact_city, df_name='fact_city')
        df_top10_rec(df=fact_city,df_name="fact_city")


    except Exception as e:
        logger.error(f"Error in main(): \n{str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()
    logger.info("presc_run_pipelines Completed!")