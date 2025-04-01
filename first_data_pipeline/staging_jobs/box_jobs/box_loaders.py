from boxsdk import Client, OAuth2
from dotenv import load_dotenv
from pymongo import MongoClient
import os
import json
import random 
import pandas as pd 


load_dotenv()

#Crear una clase que cargue files a datalake (BOX)
class BoxLoaders:
    #Inicializamos nuestra clase con las credenciales de Box
    def __init__(self):
        """Este metodo sirve para enviar archivos a BOX"""
        self.client_id = os.getenv("CLIENT_ID")
        self.client_secret = os.getenv("CLIENT_SECRET") 
        self.developer_token = os.getenv("DEVELOPER_TOKEN")
    
    def move_files_to_box(self, file_path:str, folder_id:str):
        oauth2 = OAuth2(self.client_id, self.client_secret, access_token=self.developer_token)
        client = Client(oauth2)
        upload_file = client.folder(folder_id).upload(file_path)
        print("File successfully uploaded")
    
    def get_data_from_mongo(self, MONGO_URI:str, db_name:str,
                            collection_name:str):
        mongo_client = MongoClient(MONGO_URI)
        mongo_db = mongo_client[db_name]
        mongo_collection = mongo_db[collection_name]
        mongo_data = list(mongo_collection.find({}, {"_id":0}))

        with open("tmp_files/tmp_file.json", "w") as json_file:
            for document in mongo_data:
                json.dump(document, json_file, default=str)
                json_file.write('\n')

        self.move_files_to_box("tmp_files/tmp_file.json", "311657648630")
        print('Data from Mongo loaded to BOX')


    def get_data_from_erp(self, n_sales)-> None:

        product_prices = [('Laptop A', 5400, 'Esta es una laptop A'), ('Laptop B', 3300, 'Esta es una laptop B'), 
                          ('Mouse', 150, 'Mouse ergonomico'), ('Headset', 1260, 'Audifonos gamer'), 
                          ('Pantalla', 4000, 'Pantall LED'), ('Teclado', 750, 'De los que hacen soniditos')]
        sales_random = [random.choice(product_prices) for _ in range(1, n_sales)]

        data = {'Sale ID':[id for id in range(1, n_sales)],
                'Product_Name':[sale[0] for sale in sales_random],
                'Price':[sale[1] for sale in sales_random],
                'Description':[sale[2] for sale in sales_random],
                'Quantity':[random.choice(range(1, 16)) for _ in range(1, n_sales)],
                }
        
        data_df = pd.DataFrame(data)
        data_df.to_csv(f'tmp_files/tmp_file.csv', header=True)
        self.move_files_to_box('tmp_files/tmp_file.csv', '311655750108')
        return None