from pymongo import MongoClient
from pymongo.server_api import ServerApi

# Tu cadena de conexión de MongoDB Atlas
# Asegúrate de reemplazar 'lab12' y 'losrocotos' con tus credenciales reales
# Si tu contraseña contiene caracteres especiales, asegúrate de que estén URL-encoded
CONNECTION_STRING = "mongodb+srv://lab12:losrocotos@cluster0.twg1dw1.mongodb.net/"

def connect_to_mongodb():
    """
    Establece una conexión con la base de datos MongoDB Atlas.
    """
    try:
        # Crea un nuevo cliente y conéctate al servidor
        client = MongoClient(CONNECTION_STRING, server_api=ServerApi('1'))
        
        # Envía un comando de ping para confirmar una conexión exitosa
        client.admin.command('ping')
        print("¡Conexión exitosa a MongoDB!")

        # Opcional: Lista las bases de datos para verificar
        # print("Bases de datos disponibles:")
        # for db_name in client.list_database_names():
        #     print(f"- {db_name}")

        # Puedes retornar el cliente si necesitas realizar operaciones
        return client

    except Exception as e:
        print(f"Error al conectar a MongoDB: {e}")
        return None

def main():
    client = connect_to_mongodb()
    if client:
        '''Una vez conectado, puedes acceder a tus bases de datos y colecciones
        Por ejemplo, para acceder a la base de datos 'data_casinos'
        y la colección 'load_casinos' como se ve en tu imagen:'''

        db_name = "data_casinos"
        collection_name = "load_casinos"

        try:
            db = client[db_name]
            collection = db[collection_name]
            print(f"\nAccediendo a la base de datos: {db_name}")
            print(f"Accediendo a la colección: {collection_name}")

            # Ejemplo: Contar documentos en la colección
            doc_count = collection.count_documents({})
            print(f"Número de documentos en '{collection_name}': {doc_count}")

            # Ejemplo: Encontrar un documento (opcional)
            # print("\nMostrando un documento de la colección:")
            # one_document = collection.find_one({})
            # if one_document:
            #     print(one_document)
            # else:
            #     print("No se encontraron documentos en la colección.")

        except Exception as e:
            print(f"Error al acceder a la base de datos o colección: {e}")
        finally:
            # Es buena práctica cerrar la conexión cuando termines
            client.close()
            print("Conexión cerrada.")

if __name__ == "__main__":
    main()