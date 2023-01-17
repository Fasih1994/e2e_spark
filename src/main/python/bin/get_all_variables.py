import os 

# set env vars
os.environ['envn']='TEST'
os.environ['header']='True'
os.environ['inferSchema']='True'

# get env vars

envn = os.environ['envn']
header = bool(os.environ['header'])
inferSchema = bool(os.environ['inferSchema'])

appName = "Prescriber Research Report"
data_path = 'hdfs://nodemaster:9000/data'
stagging_dim_city = os.path.join(data_path,'USA_Presc_Medicare_data_12021.csv')
stagging_fact = os.path.join(data_path,'us_cities_dimension.parquet')
