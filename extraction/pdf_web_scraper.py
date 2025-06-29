import pandas as pd
import configparser
import os
import requests
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import time
import glob

from database.db_connections import get_db

def wait_for_download_completion(download_dir, expected_file_name, timeout=60, check_interval=1, stable_checks=3):
    start_time = time.time()
    downloaded_file_path = None
    full_expected_path = os.path.join(download_dir, expected_file_name)
    temp_crdownload_path = full_expected_path + '.crdownload'
    
    file_sizes = {} 

    print(f"         ⏳ Esperando que '{expected_file_name}' se descargue en '{download_dir}'...")

    while time.time() - start_time < timeout:
        
        # 1. Verificar si el archivo final esperado ya existe y su tamaño es estable
        if os.path.exists(full_expected_path):
            try:
                current_size = os.path.getsize(full_expected_path)
                
                if full_expected_path in file_sizes and file_sizes[full_expected_path]['last_size'] == current_size:
                    file_sizes[full_expected_path]['stable_count'] += 1
                else:
                    file_sizes[full_expected_path] = {'last_size': current_size, 'stable_count': 0}
                
                if file_sizes[full_expected_path]['stable_count'] >= stable_checks:
                    downloaded_file_path = full_expected_path
                    print(f"         ✨ Descarga completada y estable: {expected_file_name} ({current_size} bytes)")
                    return downloaded_file_path
            except OSError:
                pass
        
        # 2. Verificar si el archivo .crdownload desapareció (indicando que la descarga finalizó)
        if os.path.exists(temp_crdownload_path):
            pass
        elif not os.path.exists(temp_crdownload_path) and os.path.exists(full_expected_path):
            try:
                current_size = os.path.getsize(full_expected_path)
                if full_expected_path in file_sizes and file_sizes[full_expected_path]['last_size'] == current_size:
                    file_sizes[full_expected_path]['stable_count'] += 1
                else:
                    file_sizes[full_expected_path] = {'last_size': current_size, 'stable_count': 0}
                
                if file_sizes[full_expected_path]['stable_count'] >= 1: 
                    downloaded_file_path = full_expected_path
                    print(f"         ✨ .crdownload desapareció y archivo final estable: {expected_file_name} ({current_size} bytes)")
                    return downloaded_file_path
            except OSError:
                pass 
            
        time.sleep(check_interval) 

    print(f"         ⚠️ Tiempo de espera agotado ({timeout}s) para la descarga de '{expected_file_name}'.")
    return None


def scrape_resoluciones_from_df():
    config = configparser.ConfigParser()
    config.read('configs/settings.ini')

    resoluciones_url = config['SCRAPING_PDF']['CASINOS_PDF_URL']
    collection_name = config['MONGO']['COLLECTION_LOAD']
    output_path = config['DATA_OUTPUT']['PROCESSED_PATH']
    download_dir = config['DATA_OUTPUT']['DOWNLOAD_PATH']

    os.makedirs(output_path, exist_ok=True)
    os.makedirs(download_dir, exist_ok=True)

    excel_filename = "data_casinos_pdf.xlsx"
    output_excel_path = os.path.join(output_path, excel_filename)

    print("=" * 49)
    print("==  INICIO DEL PROCESO DE SCRAPING DE RESOLUCIONES  ==")
    print("=" * 49)

    print(f"Configuración de rutas:")
    print(f"  Ruta de salida (Excel): {os.path.abspath(output_path)}")
    print(f"  Ruta de descarga (PDFs): {os.path.abspath(download_dir)}")
    print("-" * 49)

    db = get_db()
    if db is None:
        print("❌ No se pudo conectar a MongoDB.")
        pd.DataFrame(columns=['Resolución', 'Anio', 'PDF-name', 'PDF-Path', 'Código Sala']).to_excel(output_excel_path, index=False)
        return

    collection = db[collection_name]
    resoluciones_df = pd.DataFrame(list(collection.find({}, {"Resolución": 1, "Código Sala": 1, "_id": 0})))

    if resoluciones_df.empty:
        print("⚠️ No se encontraron resoluciones.")
        pd.DataFrame(columns=['Resolución', 'Anio', 'PDF-name', 'PDF-Path', 'Código Sala']).to_excel(output_excel_path, index=False)
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
    resoluciones_df['PDF-Path'] = "NO DATA"

    print(resoluciones_df.head())
    
    # Limitar el procesamiento a un número específico de filas para prueba:
    # Descomenta la siguiente línea y ajusta el número (ej. .head(20))
    resoluciones_df_to_process = resoluciones_df.copy()
    
    # Comenta la siguiente línea cuando quieras procesar todos los registros
    # resoluciones_df_to_process = resoluciones_df.copy()
    
    print(f"✅ Preprocesamiento completado. Total de {len(resoluciones_df_to_process)} filas.")

    print("🌐 Iniciando navegador...")
    driver = None
    try:
        chrome_options = Options()
        prefs = {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "plugins.always_open_pdf_externally": True
        }
        chrome_options.add_experimental_option("prefs", prefs)

        # chrome_options.add_argument("--headless") # Comenta esta línea para ver el navegador
        # chrome_options.add_argument("--start-maximized")
        # chrome_options.add_experimental_option("detach", True) 

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.get(resoluciones_url)

        # XPATH más general para la tabla de resultados. 
        # Busca cualquier tabla que esté dentro de un td con class="trans_td"
        # Esto es más robusto si la posición de tr[3] cambia.
        RESULT_TABLE_XPATH = '//td[@class="trans_td"]/table/tbody' 

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
            downloaded_paths_for_rd = []

            try:
                # Re-localizar input_field y search_button en cada iteración
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
                    # Esperar la visibilidad de la tabla de resultados. 
                    # Aumentado el tiempo de espera por si la página es lenta.
                    WebDriverWait(driver, 20).until( 
                        EC.visibility_of_element_located((By.XPATH, RESULT_TABLE_XPATH)))
                    
                    # Nuevo XPATH para los enlaces PDF: busca cualquier <a> con class="Links" 
                    # y que su href contenga 'Imagen.aspx'.
                    # Esto es más preciso según tus capturas de pantalla.
                    pdf_link_xpath = f'{RESULT_TABLE_XPATH}//a[@class="Links" and contains(@href, "Imagen.aspx")]'
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.XPATH, pdf_link_xpath)))
                    print("   ✅ Resultados y enlaces PDF detectados dentro de la tabla.")
                except Exception as e_wait_results:
                    print(f"   ⚠️ No se detectaron resultados o enlaces PDF para '{rd_value}' (Error: {e_wait_results}).")
                    # Si no se encontraron elementos en el WebDriverWait, no hay necesidad de buscar de nuevo
                    # con driver.find_elements, ya que no estarán presentes.
                    pdf_link_elements = [] # Asegura que la lista esté vacía para no intentar iterar
                
                # Si el WebDriverWait tuvo éxito, entonces busca los elementos.
                # Si falló, pdf_link_elements ya estará vacío.
                if 'pdf_link_elements' not in locals() or not pdf_link_elements:
                    pdf_link_elements = driver.find_elements(By.XPATH, pdf_link_xpath)

                if pdf_link_elements:
                    for link_element in pdf_link_elements:
                        pdf_name_from_text = link_element.text.strip()
                        pdf_url = link_element.get_attribute('href')

                        if pdf_name_from_text and pdf_url:
                            print(f"      ➡️ Encontrado texto de enlace: '{pdf_name_from_text}' con URL: '{pdf_url}'")
                            found_pdf_names.append(pdf_name_from_text)

                            # Modificación para un nombre de archivo más robusto basado en el texto del enlace
                            # Asegura que el nombre sea seguro para el sistema de archivos
                            base_name = pdf_name_from_text.replace('RD Nº ', '').replace(' - MINCETUR/DGJCMT', '').strip()
                            safe_file_name = "".join([c for c in base_name if c.isalnum() or c in (' ', '.', '_', '-')]).replace(' ', '_').rstrip()
                            if not safe_file_name.lower().endswith('.pdf'):
                                safe_file_name += '.pdf'
                            
                            expected_pdf_path = os.path.join(download_dir, safe_file_name)

                            print(f"      ⬇️ Intentando descargar: {safe_file_name}")
                            
                            downloaded_file = None
                            try:
                                link_element.click()
                                downloaded_file = wait_for_download_completion(download_dir, safe_file_name, timeout=60) 

                                if not downloaded_file:
                                    print(f"         ❌ No se confirmó la descarga automática de: {safe_file_name} vía Selenium dentro del tiempo. Intentando con requests.")
                                    response = requests.get(pdf_url, stream=True, verify=False)
                                    response.raise_for_status()

                                    with open(expected_pdf_path, 'wb') as f:
                                        for chunk in response.iter_content(chunk_size=8192):
                                            f.write(chunk)
                                    print(f"         ✅ Descargado (vía requests): {safe_file_name}")
                                    downloaded_file = expected_pdf_path 

                            except Exception as download_e:
                                print(f"         ❌ Error al intentar descarga con Selenium/Requests '{safe_file_name}' de '{pdf_url}': {download_e}")
                                downloaded_file = None 
                            
                            if downloaded_file:
                                downloaded_paths_for_rd.append(downloaded_file)
                            else:
                                print(f"         ⚠️ Descarga FALLIDA para {safe_file_name}. No se añadirá ruta.")


                    if found_pdf_names:
                        resoluciones_df.loc[original_mongo_index, 'PDF-name'] = " - ".join(found_pdf_names)
                        print(f"   ✅ 'PDF-name' actualizado para '{rd_value}': '{resoluciones_df.loc[original_mongo_index, 'PDF-name']}'")
                    
                    if downloaded_paths_for_rd:
                        resoluciones_df.loc[original_mongo_index, 'PDF-Path'] = " - ".join(downloaded_paths_for_rd)
                        print(f"   📂 Rutas PDF descargadas para '{rd_value}': {', '.join(downloaded_paths_for_rd)}")
                    else:
                        resoluciones_df.loc[original_mongo_index, 'PDF-Path'] = "NO DATA"
                        print(f"   ⚠️ No se pudieron descargar PDFs para '{rd_value}'. 'PDF-Path' se mantiene como 'NO DATA'.")

                else:
                    print("      No se encontraron enlaces PDF para esta RD en la tabla.")
                    print(f"   ⚠️ No se encontraron resultados o enlaces PDF para '{rd_value}'. 'PDF-name' y 'PDF-Path' se mantienen como 'NO DATA'.")


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
        
        try:
            final_df = pd.read_excel(output_excel_path)
        except FileNotFoundError:
            final_df = pd.DataFrame(columns=['Resolución', 'Anio', 'PDF-name', 'PDF-Path', 'Código Sala'])

        for col in ['PDF-name', 'PDF-Path']:
            if col not in final_df.columns:
                final_df[col] = "NO DATA"
        
        # Iterar sobre las filas procesadas y actualizar el final_df basado en la columna 'Resolución'
        for index, row in resoluciones_df.iterrows():
            res_val = row['Resolución']
            # Buscar la fila correspondiente en final_df
            match_index = final_df[final_df['Resolución'] == res_val].index
            if not match_index.empty:
                # Actualizar las columnas 'PDF-name' y 'PDF-Path'
                final_df.loc[match_index, 'PDF-name'] = row['PDF-name']
                final_df.loc[match_index, 'PDF-Path'] = row['PDF-Path']
        
        final_df.to_excel(output_excel_path, sheet_name="Data", index=False)
        print(f"\n✅ DataFrame FINAL exportado con éxito a: {output_excel_path}")
    except PermissionError as e:
        print(f"❌ Error de Permiso al exportar el DataFrame a Excel: {e}")
        print("Por favor, asegúrate de que el archivo no esté abierto y que tienes permisos de escritura.")
    except Exception as e:
        print(f"❌ Error inesperado al exportar el DataFrame a Excel: {e}")