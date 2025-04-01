from boxsdk import Client, OAuth2
from dotenv import load_dotenv
import os

load_dotenv()
#Definimos las variables de entorno
developer_token = os.getenv("DEVELOPER_TOKEN")
client_id = os.getenv("CLIENT_ID")
client_secret = os.getenv("CLIENT_SECRET")

#Autenticación
oauth2 = OAuth2(client_id=client_id, client_secret=client_secret, access_token=developer_token)

#Client
client = Client(oauth2)
items = client.folder('0').get_items()
if __name__ == '__main__':
    for item in items:
        print(f'Folder name{item.name} and Folder ID {item.id}')



