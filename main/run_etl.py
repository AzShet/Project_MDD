# Importamos la función principal desde nuestro módulo de extracción
from extraction.web_table_scraper import scrape_and_load_web_tables

def main():
    """
    Función principal que ejecuta el pipeline de ETL.
    """
    print("=============================================")
    print("==   INICIO DEL PIPELINE DE ETL - CASINOS  ==")
    print("=============================================")
    
    try:
        # Ejecutamos el proceso de scraping y carga de la tabla web
        scrape_and_load_web_tables()
        
        print("\n=============================================")
        print("==    PIPELINE DE ETL FINALIZADO CON ÉXITO   ==")
        print("=============================================")
        
    except Exception as e:
        print(f"\n❌ Ocurrió un error fatal en el pipeline: {e}")
        print("=============================================")
        print("==      PIPELINE DE ETL INTERRUMPIDO       ==")
        print("=============================================")


# Este bloque asegura que el código dentro de él solo se ejecute
# cuando el script es llamado directamente.
if __name__ == "__main__":
    main()