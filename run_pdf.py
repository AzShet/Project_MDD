# Importa la función de scraping de resoluciones desde extraction.pdf_web_scraper
from extraction.pdf_web_scraper import scrape_resoluciones_from_df # <-- CAMBIO AQUÍ

def main():
    """
    Función principal para ejecutar el proceso de scraping de resoluciones.
    """
    print("===============================================")
    print("== INICIO DE LA EJECUCIÓN DE SCRAPING DE RESOLUCIONES ==")
    print("===============================================")

    try:
        scrape_resoluciones_from_df() # <-- CAMBIO AQUÍ

        print("\n===============================================")
        print("== PROCESO DE SCRAPING DE RESOLUCIONES FINALIZADO CON ÉXITO ==")
        print("===============================================")

    except Exception as e:
        print(f"\n❌ Ocurrió un error fatal durante el scraping de resoluciones: {e}")
        print("===============================================")
        print("== PROCESO DE SCRAPING DE RESOLUCIONES INTERRUMPIDO ==")
        print("===============================================")

if __name__ == "__main__":
    main()