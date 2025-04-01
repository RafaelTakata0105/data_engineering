#Rafael Takata García

#750625

import pymysql
import json
import csv
from dotenv import load_dotenv
import os

load_dotenv()

class MySQLoaders:

    def __init__(self):
        self.host = os.getenv('HOST')
        self.user = os.getenv('USER')
        self.password = os.getenv('PASSWORD')
        self.database = os.getenv('DATABASE')
        # Conexión
        self.conn = pymysql.connect(host=self.host, user=self.user,
                                    password=self.password, database=self.database)
        self.cursor = self.conn.cursor()

    def truncate_table(self):
        self.conn.ping(reconnect=True)
        self.cursor.execute('TRUNCATE raw.orders')
        self.conn.commit()
        self.conn.close()

    def insert_from_json(self):
        # Leer el JSON e insertar a la base de datos
        self.conn.ping(reconnect=True)
        with open("tmp_files/tmp_file.json", "r", encoding='utf-8') as json_file:
            for document in json_file:
                row = json.loads(document)
                self.cursor.execute("INSERT INTO raw.orders (name, price, description_product, quantity) VALUES(%s, %s, %s, %s)",
                                    (row['product']['name'], row['product']['price'], row['product']['description'], row['quantity']))
        self.conn.commit()
        self.conn.close()
        print('Datos insertados en MySQL desde Mongo')

    def insert_from_csv(self):
        #Leer datos desde un csv e insertarlos a mysql
        self.conn.ping(reconnect=True)
        with open("tmp_files/tmp_file.csv", "r", encoding='utf-8') as csv_file:
            csv_reader = csv.reader(csv_file)
            headers = next(csv_reader)
            for row in csv_reader:
                self.cursor.execute("INSERT INTO raw.orders (name, price, description_product, quantity) VALUES(%s, %s, %s, %s)",
                                    (row[0], row[1], row[2], row[3]))
        self.conn.commit()
        self.conn.close()
        print('Datos insertados en MySQL desde ERP')
                
                
