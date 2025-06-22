from pymongo import MongoClient
from pymongo.server_api import ServerApi

CONNECTION_STRING = "mongodb+srv://usuario:password@cluster.mongodb.net/"
DB_NAME = "data_casinos"

def get_db():
    try:
        client = MongoClient(CONNECTION_STRING, server_api=ServerApi('1'))
        client.admin.command("ping")
        print("✅ Conectado a MongoDB")
        return client[DB_NAME]
    except Exception as e:
        print(f"❌ Error de conexión: {e}")
        return None
    
