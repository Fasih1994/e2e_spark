def validate_spark_object(spark=None):
    try:

        date = spark.sql('select current_date')
        print(f"Validating spark bject by current date - {date.collect()}")

    except Exception as e:
        print(f"Error in the method validate_spark_object \n {str(e)}")