## IMport everything
import sys
import get_all_variables as var
from creat_object import get_spark_object
from validations import validate_spark_object
import logging

def main():
    try:

        spark = get_spark_object(var.envn,var.appName)
        print("Spark object created!")
        validate_spark_object(spark=spark)
        print(sbak)

    except Exception as e:
        print(f"Error in main(): \n{str(e)}")
        sys.exit(1)

if __name__ == '__main__':
    main()
