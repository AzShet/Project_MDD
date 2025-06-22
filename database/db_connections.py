import configparser
from pymongo import MongoClient
from pymongo.server_api import ServerApi

def get_db():
    """
    Establece conexión con la base de datos MongoDB utilizando la configuración
    del archivo settings.ini.

    Returns:
        Db: Objeto de la base de datos de MongoDB si la conexión es exitosa.
        None: Si la conexión falla.
    """
    # Leer la configuración desde el archivo .ini
    config = configparser.ConfigParser()
    config.read('configs/settings.ini')

    # Obtener credenciales y nombre de la BD
    connection_string = config['MONGO']['CONNECTION_STRING']
    db_name = config['MONGO']['DB_NAME']

    try:
        # Crear un nuevo cliente y conectar al servidor
        client = MongoClient(connection_string, server_api=ServerApi('1'))
        
        # Enviar un ping para confirmar una conexión exitosa
        client.admin.command("ping")
        print("✅ Conexión a MongoDB exitosa.")
        
        return client[db_name]
    
    except Exception as e:
        print(f"❌ Error al conectar con MongoDB: {e}")
        return None