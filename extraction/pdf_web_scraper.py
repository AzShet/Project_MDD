import pandas as pd
import configparser
import os
import requests # Necesario para descargar archivos si el método de Selenium falla o si se prefiere una descarga directa
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time

# Importamos nuestra función de conexión a DB
from database.db_connections import get_db

def scrape_resoluciones_from_df():
    config = configparser.ConfigParser()
    config.read('configs/settings.ini')

    resoluciones_url = config['SCRAPING_PDF']['CASINOS_PDF_URL']
    collection_name = config['MONGO']['COLLECTION_LOAD']
    output_path = config['DATA_OUTPUT']['PROCESSED_PATH']
    # Nueva configuración para la ruta de descarga de PDFs
    download_dir = config['DATA_OUTPUT']['DOWNLOAD_PATH'] # Asumiendo que tienes esta key en settings.ini

    os.makedirs(output_path, exist_ok=True)
    os.makedirs(download_dir, exist_ok=True) # Crear el directorio de descarga si no existe

    excel_filename = "data_casinos_pdf.xlsx"
    output_excel_path = os.path.join(output_path, excel_filename)

    print("=" * 49)
    print("==  INICIO DEL PROCESO DE SCRAPING DE RESOLUCIONES  ==")
    print("=" * 49)

    db = get_db()
    if db is None:
        print("❌ No se pudo conectar a MongoDB.")
        pd.DataFrame(columns=['Resolución', 'Anio', 'PDF-name', 'Código Sala']).to_excel(output_excel_path, index=False)
        return

    collection = db[collection_name]
    resoluciones_df = pd.DataFrame(list(collection.find({}, {"Resolución": 1, "Código Sala": 1, "_id": 0})))

    if resoluciones_df.empty:
        print("⚠️ No se encontraron resoluciones.")
        pd.DataFrame(columns=['Resolución', 'Anio', 'PDF-name', 'Código Sala']).to_excel(output_excel_path, index=False)
        return

    print("⚙️ Preprocesando la columna 'Resolución'...")
    resoluciones_df['Resolución'] = resoluciones_df['Resolución'].astype(str).str.strip()
    resoluciones_df['Original_Index'] = resoluciones_df.index

    temp_df = resoluciones_df.copy()
    split_data = temp_df['Resolución'].str.rsplit('-', n=1, expand=True)
    temp_df['Anio'] = None

    if split_data.shape[1] > 1:
        temp_df['Resolucion_Num'] = split_data[0].str.strip()
        temp_df['Anio_Extracted'] = split_data[1].str.strip()
        temp_df.loc[temp_df['Anio_Extracted'].str.match(r'^\d{4}$', na=False), 'Anio'] = temp_df['Anio_Extracted']
        temp_df.loc[temp_df['Anio'].notna(), 'Resolución'] = temp_df['Resolucion_Num']
        temp_df = temp_df.drop(columns=['Resolucion_Num', 'Anio_Extracted'])

    resoluciones_df = temp_df
    resoluciones_df['PDF-name'] = "NO DATA"

    print(resoluciones_df.head())
    print(f"✅ Preprocesamiento completado. Total de {len(resoluciones_df)} filas.")

    # Limitar a 5 filas para prueba (descomenta para pruebas, comenta para producción)
    # resoluciones_df_to_process = resoluciones_df.head(5).copy()
    resoluciones_df_to_process = resoluciones_df.copy() # Procesa todo el DataFrame

    print("🌐 Iniciando navegador...")
    driver = None
    try:
        chrome_options = Options()
        # Configurar preferencias de descarga de Chrome
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False, # Deshabilita la pregunta de descarga
            "plugins.always_open_pdf_externally": True # Abrir PDFs externamente en lugar de en el navegador
        }
        chrome_options.add_experimental_option("prefs", prefs)

        # chrome_options.add_argument("--headless") # Comenta para ver el navegador
        # chrome_options.add_argument("--start-maximized")
        # chrome_options.add_experimental_option("detach", True)

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.get(resoluciones_url)

        # Definir el XPath de la tabla de resultados
        RESULT_TABLE_XPATH = '//*[@id="form1"]/table/tbody/tr[3]/td/table'

        def initialize_page_elements(driver_instance):
            print("⚙️ Inicializando elementos...")
            WebDriverWait(driver_instance, 30).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="TB_NRO_RESO"]')))

            dispositivo_selector = Select(WebDriverWait(driver_instance, 15).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="UC_TIP_DISP_OBJ_DDL_COD_DISP"]'))))
            dispositivo_selector.select_by_value("5")
            print("✅ Filtro 'RESOLUCIÓN DIRECTORAL' seleccionado.")

            anio_selector = Select(WebDriverWait(driver_instance, 15).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="UC_ANO_OBJ_DDL_ANO_PROC"]'))))

            WebDriverWait(driver_instance, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="UC_ANO_OBJ_DDL_ANO_PROC"]/option[text()="--Todos--"]')))
            anio_selector.select_by_visible_text("--Todos--")
            print("✅ Filtro de Año '--TODOS--' seleccionado.")
            time.sleep(1)

            input_field = WebDriverWait(driver_instance, 10).until(
                EC.presence_of_element_located((By.XPATH, '//*[@id="TB_NRO_RESO"]')))
            search_button = WebDriverWait(driver_instance, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="BUSCAR"]')))

            return input_field, search_button, anio_selector

        input_field, search_button, anio_selector = initialize_page_elements(driver)

        for index, row in resoluciones_df_to_process.iterrows():
            rd_value = row['Resolución']
            original_mongo_index = row['Original_Index']
            print(f"\nProcesando {index+1}/{len(resoluciones_df_to_process)}: RD '{rd_value}'")
            found_pdf_names = []
            downloaded_files_for_rd = [] # Para almacenar los nombres de los archivos descargados para esta RD

            try:
                # Re-localizar input_field y search_button en cada iteración
                # Esto ayuda a prevenir StaleElementReferenceException si la página se actualiza
                input_field = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, '//*[@id="TB_NRO_RESO"]')))
                search_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//*[@id="BUSCAR"]')))

                input_field.clear()
                input_field.send_keys(rd_value)
                print(f"   ✍️ Valor '{rd_value}' escrito.")
                search_button.click()
                print("   🔍 Botón 'Buscar' presionado.")

                try:
                    WebDriverWait(driver, 10).until(
                        EC.visibility_of_element_located((By.XPATH, RESULT_TABLE_XPATH)))
                    
                    # Ahora volvemos a incluir la condición de .pdf para la descarga
                    WebDriverWait(driver, 5).until(
                        EC.presence_of_all_elements_located((By.XPATH, f'{RESULT_TABLE_XPATH}//a[@class="Links" and contains(@href, ".pdf")]')))
                    print("   ✅ Resultados y enlaces PDF detectados dentro de la tabla.")
                except Exception as e_wait_results:
                    print(f"   ⚠️ No se detectaron resultados o enlaces PDF para '{rd_value}' (Error: {e_wait_results}).")
                    
                # Ahora volvemos a incluir la condición de .pdf para la descarga
                pdf_link_elements = driver.find_elements(By.XPATH, f'{RESULT_TABLE_XPATH}//a[@class="Links" and contains(@href, ".pdf")]')
                
                if pdf_link_elements:
                    for link_element in pdf_link_elements:
                        pdf_name_from_text = link_element.text.strip()
                        pdf_url = link_element.get_attribute('href') # Obtener la URL del PDF

                        if pdf_name_from_text and pdf_url:
                            print(f"      ➡️ Encontrado texto de enlace: '{pdf_name_from_text}' con URL: '{pdf_url}'")
                            found_pdf_names.append(pdf_name_from_text)

                            # --- Lógica de descarga del PDF ---
                            # Sanear el nombre del archivo para que sea válido en el sistema de archivos
                            safe_file_name = "".join([c for c in pdf_name_from_text if c.isalnum() or c in (' ', '.', '_', '-')]).rstrip()
                            if not safe_file_name.lower().endswith('.pdf'):
                                safe_file_name += '.pdf'
                            pdf_path = os.path.join(download_dir, safe_file_name)

                            print(f"      ⬇️ Intentando descargar: {safe_file_name}")

                            try:
                                # Selenium no tiene un método directo click-and-wait-for-download
                                # La configuración de prefs debería manejar la descarga al hacer clic
                                link_element.click()
                                time.sleep(3) # Espera un poco para que la descarga inicie/complete. Ajustar si es necesario.

                                # Opcional: Verificar si el archivo se descargó (requiere un poco más de lógica)
                                # Para una verificación simple, podemos listar los archivos en el directorio de descarga.
                                # Más robusto sería usar expected_conditions.invisibility_of_element_located
                                # para una barra de progreso de descarga si existiera, o monitorear el directorio.
                                downloaded_files = os.listdir(download_dir)
                                if safe_file_name in downloaded_files:
                                    print(f"         ✅ Descargado: {safe_file_name}")
                                    downloaded_files_for_rd.append(safe_file_name)
                                else:
                                    print(f"         ❌ No se confirmó la descarga automática de: {safe_file_name}. Intentando con requests.")
                                    # Intentar con requests si Selenium no lo descargó automáticamente
                                    response = requests.get(pdf_url, stream=True, verify=False) # 'verify=False' si hay problemas de SSL
                                    response.raise_for_status() # Lanza un error para códigos de estado HTTP malos

                                    with open(pdf_path, 'wb') as f:
                                        for chunk in response.iter_content(chunk_size=8192):
                                            f.write(chunk)
                                    print(f"         ✅ Descargado (vía requests): {safe_file_name}")
                                    downloaded_files_for_rd.append(safe_file_name)


                            except Exception as download_e:
                                print(f"         ❌ Error al descargar '{safe_file_name}' de '{pdf_url}': {download_e}")
                                # Se deja que 'PDF-name' se actualice solo con los nombres encontrados.

                    if found_pdf_names:
                        resoluciones_df.loc[original_mongo_index, 'PDF-name'] = " - ".join(found_pdf_names)
                        print(f"   ✅ 'PDF-name' actualizado para '{rd_value}': '{resoluciones_df.loc[original_mongo_index, 'PDF-name']}'")
                    if downloaded_files_for_rd:
                        # Opcional: Podrías añadir una columna para los nombres de archivo descargados si lo necesitas
                        print(f"   📂 Archivos descargados para '{rd_value}': {', '.join(downloaded_files_for_rd)}")
                    else:
                        print(f"   ⚠️ No se encontraron PDFs para '{rd_value}' o no se pudieron descargar. 'PDF-name' se mantiene como 'NO DATA'.")

                else:
                    print("      No se encontraron enlaces PDF para esta RD en la tabla.")
                    print(f"   ⚠️ No se encontraron resultados o enlaces PDF para '{rd_value}'. 'PDF-name' se mantiene como 'NO DATA'.")


                current_state_df = resoluciones_df.copy()
                if 'Original_Index' in current_state_df.columns:
                    current_state_df.drop(columns=['Original_Index'], inplace=True)
                current_state_df.to_excel(output_excel_path, index=False)
                print(f"📝 Guardado parcial tras fila {index+1} ({rd_value}).")

            except Exception as e:
                print(f"❌ Error general al procesar RD '{rd_value}': {e}")
                print(f"   ⚠️ Error crítico para RD '{rd_value}'. Intentando recargar y continuar.")
                try:
                    driver.get(resoluciones_url)
                    # No es necesario re-inicializar input_field, search_button, anio_selector aquí
                    # ya que se re-localizan al inicio de cada iteración del bucle for.
                    # Solo necesitamos seleccionar los filtros de nuevo.
                    dispositivo_selector = Select(WebDriverWait(driver, 15).until(
                        EC.element_to_be_clickable((By.XPATH, '//*[@id="UC_TIP_DISP_OBJ_DDL_COD_DISP"]'))))
                    dispositivo_selector.select_by_value("5")
                    anio_selector = Select(WebDriverWait(driver, 15).until(
                        EC.element_to_be_clickable((By.XPATH, '//*[@id="UC_ANO_OBJ_DDL_ANO_PROC"]'))))
                    anio_selector.select_by_visible_text("--Todos--")
                    print("   ✅ Página recargada y filtros re-establecidos tras recuperación.")
                except Exception as recovery_e:
                    print(f"   ❌ Fallo en la recuperación para '{rd_value}': {recovery_e}")
                    print("   ⚠️ El script podría ser inestable. Considera detenerlo.")
                continue

    except Exception as e:
        print(f"\n❌ Ocurrió un error fatal durante el web scraping general: {e}")
    finally:
        if driver:
            driver.quit()
            print("🌐 Navegador cerrado.")

    print("\n=============================================")
    print("==  FIN DEL PROCESO DE SCRAPING DE RESOLUCIONES  ===")
    print("=============================================")

    try:
        if 'Original_Index' in resoluciones_df.columns:
            resoluciones_df.drop(columns=['Original_Index'], inplace=True)
        resoluciones_df.to_excel(output_excel_path, sheet_name="Data", index=False)
        print(f"\n✅ DataFrame FINAL exportado con éxito a: {output_excel_path}")
    except PermissionError as e:
        print(f"❌ Error de Permiso al exportar el DataFrame a Excel: {e}")
        print("Por favor, asegúrate de que el archivo no esté abierto y que tienes permisos de escritura.")
    except Exception as e:
        print(f"❌ Error inesperado al exportar el DataFrame a Excel: {e}")