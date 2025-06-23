import pandas as pd
import configparser
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time # Para pausas adicionales si son necesarias

# Importamos nuestra función de conexión a DB
from database.db_connections import get_db

def scrape_resoluciones_from_df():
    """
    Extrae números de resolución de MongoDB a un DataFrame,
    luego realiza web scraping en la página de Mincetur para cada resolución,
    escribiendo el valor en el campo de búsqueda y haciendo clic en 'Buscar'.
    """
    config = configparser.ConfigParser()
    config.read('configs/settings.ini')

    resoluciones_url = config['SCRAPING_PDF']['CASINOS_PDF_URL']
    collection_name = config['MONGO']['COLLECTION_LOAD']

    print(f"=================================================")
    print(f"==  INICIO DEL PROCESO DE SCRAPING DE RESOLUCIONES  ==")
    print(f"=================================================")

    # --- 1. Obtener DataFrame de Resoluciones desde MongoDB ---
    db = get_db()
    if db is None:
        print("❌ No se pudo conectar a MongoDB. No se pueden obtener los RDs.")
        return

    collection = db[collection_name]
    resoluciones_df = pd.DataFrame(list(collection.find({}, {"Resolución": 1, "_id": 0})))
    
    if resoluciones_df.empty:
        print("⚠️ No se encontraron resoluciones en la colección de MongoDB.")
        return
    
    resoluciones_list = resoluciones_df['Resolución'].dropna().unique().tolist()
    print(f"✅ Se encontraron {len(resoluciones_list)} resoluciones únicas para procesar.")

    # --- 2. Configuración de Selenium ---
    print("🌐 Iniciando navegador para web scraping de resoluciones...")
    driver = None
    try:
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        driver.get(resoluciones_url)

        # Esperar a que la página de consulta cargue completamente y el campo de entrada sea visible
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.XPATH, '//*[@id="TB_NRO_RESO"]'))
        )
        print("✅ Página de consulta cargada y campo de entrada detectado.")

        # --- 3. Iterar sobre las resoluciones y realizar la búsqueda ---
        for i, rd_value in enumerate(resoluciones_list):
            print(f"\nProcesando {i+1}/{len(resoluciones_list)}: Resolución '{rd_value}'")
            try:
                # Encuentra el campo de texto y límpialo antes de escribir
                input_field = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="TB_NRO_RESO"]'))
                )
                input_field.clear() # Limpiar cualquier valor anterior
                input_field.send_keys(rd_value) # Escribir el valor de la resolución

                print(f"   ✍️ Valor '{rd_value}' escrito en el campo de búsqueda.")

                # Encuentra el botón de búsqueda y haz clic
                search_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//*[@id="BUSCAR"]'))
                )
                search_button.click()
                print("   🔍 Botón 'Buscar' presionado.")

                # Opcional: Puedes añadir una pequeña pausa para observar lo que ocurre
                # o una espera para algún elemento que indique que la búsqueda se completó.
                # Por ejemplo, esperar a que algún texto cambie o un spinner desaparezca.
                # Aquí, solo una pausa corta para propósitos de prueba visual.
                time.sleep(2) 

                # Si quieres verificar que hay resultados, podrías esperar por la tabla de resultados.
                # try:
                #     WebDriverWait(driver, 10).until(
                #         EC.presence_of_element_located((By.XPATH, '//*[@id="divResultado"]//table'))
                #     )
                #     print("   ✅ Resultados de búsqueda cargados.")
                # except:
                #     print("   ⚠️ No se detectaron resultados o la tabla no cargó.")

                # Volver a la página inicial o limpiar el estado para la próxima búsqueda
                # Esto es crucial para que el campo de búsqueda esté listo para la siguiente iteración.
                driver.get(resoluciones_url)
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="TB_NRO_RESO"]'))
                )


            except Exception as e:
                print(f"❌ Error al procesar resolución '{rd_value}': {e}")
                # En caso de error, intenta volver a la página inicial para la siguiente iteración
                try:
                    driver.get(resoluciones_url)
                    WebDriverWait(driver, 30).until(
                        EC.presence_of_element_located((By.XPATH, '//*[@id="TB_NRO_RESO"]'))
                    )
                except:
                    print("   ⚠️ No se pudo reiniciar el navegador. El script podría fallar en la próxima iteración.")
                continue # Continúa con la siguiente resolución incluso si esta falla

    except Exception as e:
        print(f"\n❌ Ocurrió un error fatal durante el web scraping general: {e}")
    finally:
        if driver:
            driver.quit()
            print("🌐 Navegador cerrado.")

    print("\n=============================================")
    print("==  FIN DEL PROCESO DE SCRAPING DE RESOLUCIONES  ===")
    print("=============================================")