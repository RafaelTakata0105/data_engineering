from staging_jobs.box_jobs.box_loaders import BoxLoaders
from staging_jobs.mysql_jobs.mysql_loaders import MySQLoaders
from dotenv import load_dotenv
import os

loader_box = BoxLoaders()
loader_mysql = MySQLoaders()

load_dotenv()
MONGO_URI=os.getenv("MONGO_URI")

# Extracción de datos
loader_box.get_data_from_mongo(MONGO_URI=MONGO_URI, db_name="store", collection_name="orders")
loader_box.get_data_from_erp(100)

#Hacemos "Truncate and reload"
loader_mysql.truncate_table()

#Ingestión de datos
loader_mysql.insert_from_json()
loader_mysql.insert_from_csv()