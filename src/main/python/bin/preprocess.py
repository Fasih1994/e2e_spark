from pyspark.sql.functions import upper, lit, col, regexp_extract
from pyspark.sql.dataframe import DataFrame

import logging
import logging.config as log_conf

log_conf.fileConfig(fname="/home/hadoop/e2e_pyspark/src/main/python/utils/logging_to_file.conf")
logger = logging.getLogger(__name__)

def clean_data(df :DataFrame =None, df_name:str = None) -> DataFrame:
    logger.info(f"Performing data clean on {df_name}")
    try:
        if df_name=='dim_city':
            df = df.select(
                upper(df.city).alias('city'), 
                df.state_id, 
                upper(df.state_name).alias('state_name'), 
                upper(df.county_name).alias('county_name'), 
                df.population, 
                df.zips)
        elif df_name=='fact_city':
            df = df.select(
                df.npi.alias('presc_id'), df.nppes_provider_last_org_name.alias("presc_lname"),
                df.nppes_provider_first_name.alias('presc_fname'), df.nppes_provider_city.alias("presc_city"),
                df.nppes_provider_state.alias('presc_state'), df.specialty_description.alias('presc_spclt'), df.years_of_exp,
                df.drug_name, df.total_claim_count.alias('txn_cnt'), df.total_day_supply, df.total_drug_cost
            )
            df = df.withColumn('country_name', lit("USA"))
            # extract only digits and cast as intrgers
            df = df.withColumn('years_of_exp', regexp_extract(col('years_of_exp'), '\d+', idx=0).cast('int'))

    except Exception as e:
        logger.error('Error occured in clean_data', exc_info=True)
    else:
        logger.info(f'Data Clean completed for {df_name}')
    return df