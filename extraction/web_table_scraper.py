import time
import os
import configparser
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

# Importamos nuestra función de conexión a la base de datos
from database.db_connections import get_db

def scrape_and_load_web_tables():
    """
    Realiza el scraping de la tabla de salas de casino desde la web de MINCETUR,
    la procesa y la carga en la base de datos MongoDB.
    """
    # --- 1. Lectura de Configuración ---
    config = configparser.ConfigParser()
    config.read('configs/settings.ini')
    
    url_web = config['SCRAPING']['CASINOS_URL']
    output_path = config['DATA_OUTPUT']['PROCESSED_PATH']
    collection_name = config['MONGO']['COLLECTION_LOAD']

    # Asegurarse de que el directorio de salida exista
    os.makedirs(output_path, exist_ok=True)
    
    # --- 2. Configuración de Selenium ---
    print("🚀 Iniciando el proceso de scraping...")
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
    driver.get(url_web)

    try:
        # Esperar hasta que la tabla inicial cargue
        WebDriverWait(driver, 60).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#divResultadoSala table[cellspacing='0']"))
        )
        
        # --- 3. Extracción de Datos por Paginación ---
        todos_los_datos = []
        pagina = 1

        while True:
            print(f"📄 Extrayendo datos de la página {pagina}...")
            
            try:
                # Esperar a que la tabla de la página actual esté presente
                tabla = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#divResultadoSala table[cellspacing='0']"))
                )
                
                filas = tabla.find_elements(By.XPATH, ".//tr[@data-id]")
                for fila in filas:
                    celdas = fila.find_elements(By.TAG_NAME, "td")
                    datos_fila = [celda.text.strip() for celda in celdas]
                    if datos_fila:
                        todos_los_datos.append(datos_fila)
            except Exception as e:
                print(f"⚠️ No se encontró la tabla en la página {pagina} o ocurrió un error: {e}")
                break

            # Intentar ir a la página siguiente
            try:
                boton_siguiente = WebDriverWait(driver, 5).until(
                    EC.element_to_be_clickable((By.CLASS_NAME, "siguiente"))
                )
                boton_siguiente.click()
                pagina += 1
                # Espera explícita a que la tabla se recargue
                WebDriverWait(driver, 10).until(EC.staleness_of(tabla))
            except Exception:
                print("✅ Fin de la paginación. No hay más páginas.")
                break

    finally:
        # --- 4. Cierre del Navegador ---
        driver.quit()

    # --- 5. Procesamiento con Pandas ---
    if not todos_los_datos:
        print("❌ No se extrajeron datos. El proceso terminará.")
        return

    columnas = [
        "Ruc", "Empresa", "Establecimiento", "Giro", "Resolución",
        "Código Sala", "Vigencia", "Dirección", "Distrito", "Provincia", "Departamento"
    ]
    df = pd.DataFrame(todos_los_datos, columns=columnas)
    
    # Guardar los datos en archivos locales (CSV y Excel)
    df.to_csv(os.path.join(output_path, "datos_casinos_salas.csv"), index=False, encoding='utf-8-sig')
    df.to_excel(os.path.join(output_path, "datos_casinos_salas.xlsx"), sheet_name="Data", index=False)

    print(f"\n✅ Total de filas extraídas: {len(df)}")
    print("Primeras 5 filas del DataFrame:")
    print(df.head())

    # --- 6. Carga a MongoDB ---
    db = get_db()
    if db:
        collection = db[collection_name]
        documentos = df.to_dict(orient="records")
        
        if documentos:
            # Para evitar duplicados, es una buena práctica borrar los datos antiguos antes de una nueva carga
            print(f"Borrando datos antiguos de la colección '{collection_name}'...")
            collection.delete_many({})
            
            print(f"Cargando {len(documentos)} nuevos documentos...")
            collection.insert_many(documentos)
            print(f"✅ {len(documentos)} documentos insertados exitosamente en '{collection_name}'")
        else:
            print("⚠️ No hay datos para insertar en la base de datos.")