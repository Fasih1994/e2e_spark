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
current_path = os.getcwd()
stagging_dim_city = os.path.join(current_path,'stagging', 'dimension_city')
stagging_fact = os.path.join(current_path,'stagging', 'fact')
