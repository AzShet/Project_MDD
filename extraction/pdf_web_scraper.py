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
import zipfile # Para manejar archivos ZIP
import shutil # Para copiar archivos dentro de la descompresión
import re # Para expresiones regulares en nombres de archivo
from urllib.parse import unquote # Para decodificar nombres de archivo de URLs
# casi
# Importamos nuestra función de conexión a DB 
from database.db_connections import get_db

def unzip_pdfs(zip_path, extract_to_dir):
    """
    Descomprime un archivo ZIP y busca archivos PDF dentro.
    Retorna una lista de rutas absolutas de los PDFs extraídos.
    """
    extracted_pdf_paths = []
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_contents = zip_ref.namelist()
            print(f"         📦 Contenido del ZIP '{os.path.basename(zip_path)}': {zip_contents}")

            for member in zip_contents:
                if member.lower().endswith('.pdf'):
                    extracted_pdf_name = os.path.basename(member) 
                    extracted_pdf_path = os.path.join(extract_to_dir, extracted_pdf_name)
                    
                    # Evitar nombres de archivo duplicados al extraer
                    counter = 1
                    original_extracted_pdf_path = extracted_pdf_path
                    while os.path.exists(extracted_pdf_path):
                        name, ext = os.path.splitext(original_extracted_pdf_path)
                        extracted_pdf_path = f"{name}_{counter}{ext}"
                        counter += 1

                    print(f"         📄 Extrayendo PDF: '{member}' a '{os.path.basename(extracted_pdf_path)}'")
                    
                    source = zip_ref.open(member)
                    target = open(extracted_pdf_path, "wb")
                    with source, target:
                        shutil.copyfileobj(source, target)

                    extracted_pdf_paths.append(extracted_pdf_path)
                else:
                    print(f"         ➖ Ignorando archivo no-PDF en ZIP: '{member}'")
        
        # Opcional: Eliminar el archivo ZIP después de la extracción
        print(f"         🗑️ Eliminando archivo ZIP: '{os.path.basename(zip_path)}'")
        os.remove(zip_path)

    except zipfile.BadZipFile:
        print(f"         ❌ Error: El archivo '{os.path.basename(zip_path)}' no es un archivo ZIP válido.")
    except Exception as e:
        print(f"         ❌ Error al descomprimir ZIP '{os.path.basename(zip_path)}': {e}")
    
    return extracted_pdf_paths

def wait_for_download_completion(download_dir, timeout=60, check_interval=1, stable_checks=3):
    """
    Espera a que un archivo se descargue en el directorio dado,
    que no sea un archivo .crdownload, y que su tamaño se estabilice.
    Retorna la ruta completa del archivo descargado o None si hay timeout.
    No requiere un nombre de archivo específico inicialmente, busca el primero nuevo y estable.
    """
    start_time = time.time()
    downloaded_file_path = None
    
    # Obtener la lista de archivos existentes antes de la descarga
    initial_files = set(os.listdir(download_dir))
    
    file_sizes = {} # Para rastrear el tamaño del archivo y detectar estabilidad

    print(f"         ⏳ Esperando que un archivo se descargue en '{download_dir}'...")

    while time.time() - start_time < timeout:
        current_files = set(os.listdir(download_dir))
        new_files = current_files - initial_files

        for file_name in new_files:
            full_path = os.path.join(download_dir, file_name)
            
            # Ignorar archivos temporales de descarga (.crdownload, .tmp, etc.)
            if file_name.lower().endswith(('.crdownload', '.tmp', '.part')):
                continue

            try:
                current_size = os.path.getsize(full_path)
                
                if full_path in file_sizes and file_sizes[full_path]['last_size'] == current_size:
                    file_sizes[full_path]['stable_count'] += 1
                else:
                    file_sizes[full_path] = {'last_size': current_size, 'stable_count': 0}
                
                if file_sizes[full_path]['stable_count'] >= stable_checks:
                    print(f"         ✨ Descarga completada y estable: {file_name} ({current_size} bytes)")
                    return full_path # Retorna la ruta del archivo descargado
            except OSError:
                # El archivo puede estar siendo escrito, inaccesible temporalmente
                pass
        
        time.sleep(check_interval)

    print(f"         ⚠️ Tiempo de espera agotado ({timeout}s) para la descarga de un archivo.")
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
    
    # === Procesar TODOS los registros ===
    # Comentar la línea para limitar los registros de prueba
    # resoluciones_df_to_process = resoluciones_df.head(20).copy() 
    
    # Descomentar la línea para procesar TODOS los registros
    resoluciones_df_to_process = resoluciones_df.copy()
    # ====================================
    
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

        # Puedes descomentar la siguiente línea para ejecutar el navegador en segundo plano (sin interfaz gráfica)
        # chrome_options.add_argument("--headless") 
        # chrome_options.add_argument("--start-maximized")
        # chrome_options.add_experimental_option("detach", True) 

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        driver.get(resoluciones_url)

        # XPATH más general para la tabla de resultados. 
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
                    WebDriverWait(driver, 20).until( 
                        EC.visibility_of_element_located((By.XPATH, RESULT_TABLE_XPATH)))
                    
                    # Nuevo XPATH para los enlaces: busca cualquier <a> con class="Links" 
                    # y que su href contenga 'Imagen.aspx'.
                    pdf_link_xpath = f'{RESULT_TABLE_XPATH}//a[@class="Links" and contains(@href, "Imagen.aspx")]'
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_all_elements_located((By.XPATH, pdf_link_xpath)))
                    print("   ✅ Resultados y enlaces PDF detectados dentro de la tabla.")
                except Exception as e_wait_results:
                    print(f"   ⚠️ No se detectaron resultados o enlaces (PDF/ZIP) para '{rd_value}' (Error: {e_wait_results}).")
                    pdf_link_elements = [] 
                
                if 'pdf_link_elements' not in locals() or not pdf_link_elements:
                    pdf_link_elements = driver.find_elements(By.XPATH, pdf_link_xpath)


                if pdf_link_elements:
                    for link_element in pdf_link_elements:
                        pdf_name_from_text = link_element.text.strip()
                        pdf_url = link_element.get_attribute('href')

                        if pdf_name_from_text and pdf_url:
                            print(f"      ➡️ Encontrado texto de enlace: '{pdf_name_from_text}' con URL: '{pdf_url}'")
                            found_pdf_names.append(pdf_name_from_text)

                            # Extraer un nombre base seguro del texto del enlace
                            base_name_from_text = pdf_name_from_text.replace('RD Nº ', '').replace(' - MINCETUR/DGJCMT', '').strip()
                            safe_base_name = "".join([c for c in base_name_from_text if c.isalnum() or c in (' ', '.', '_', '-')]).replace(' ', '_').rstrip('_')
                            
                            print(f"      ⬇️ Intentando descargar el archivo asociado a: {pdf_name_from_text}")
                            
                            downloaded_file = None
                            try:
                                # Haz clic en el enlace para iniciar la descarga por el navegador
                                link_element.click()
                                
                                # Monitorea el directorio de descarga en general
                                downloaded_file = wait_for_download_completion(download_dir, timeout=60) 
                                
                                if not downloaded_file:
                                    print(f"         ❌ No se confirmó la descarga automática de un archivo vía Selenium dentro del tiempo. Intentando con requests.")
                                    response = requests.get(pdf_url, stream=True, verify=False)
                                    response.raise_for_status()
                                    
                                    # Determinar el nombre del archivo desde la cabecera Content-Disposition si es posible
                                    download_filename = None
                                    content_disposition = response.headers.get('Content-Disposition')
                                    if content_disposition:
                                        fname_match = re.findall(r'filename\*?=(?:UTF-8\'\')?\"?([^\"]+)\"?', content_disposition)
                                        if fname_match:
                                            try:
                                                download_filename = unquote(fname_match[0], encoding='utf-8')
                                            except:
                                                download_filename = unquote(fname_match[0])
                                    
                                    # Si no se obtuvo de Content-Disposition, intentar del URL o usar el nombre base
                                    if not download_filename:
                                        url_filename = os.path.basename(pdf_url.split('?')[0])
                                        if '.' in url_filename and len(url_filename.split('.')[-1]) <= 5: # Intenta obtener extensión si parece un nombre de archivo
                                            download_filename = url_filename
                                        else:
                                            # Fallback a un nombre usando el safe_base_name y una extensión por defecto
                                            download_filename = f"{safe_base_name}.pdf" # Asumimos .pdf como último recurso
                                    
                                    # Asegurarse de que el nombre de archivo sea seguro y no demasiado largo
                                    download_filename = "".join([c for c in download_filename if c.isalnum() or c in (' ', '.', '_', '-')]).replace(' ', '_').rstrip('_')

                                    # Si el nombre de archivo no tiene extensión, añadir una por defecto
                                    if not '.' in download_filename:
                                        download_filename += '.pdf'


                                    downloaded_file = os.path.join(download_dir, download_filename)

                                    with open(downloaded_file, 'wb') as f:
                                        for chunk in response.iter_content(chunk_size=8192):
                                            f.write(chunk)
                                    print(f"         ✅ Descargado (vía requests): {download_filename}")

                            except Exception as download_e:
                                print(f"         ❌ Error al intentar descarga con Selenium/Requests para el enlace '{pdf_name_from_text}' de '{pdf_url}': {download_e}")
                                downloaded_file = None 
                            
                            if downloaded_file and os.path.exists(downloaded_file): # Asegurarse de que el archivo realmente existe
                                # AHORA, verificamos si el archivo descargado es un ZIP
                                if downloaded_file.lower().endswith('.zip'):
                                    print(f"         🧩 Archivo descargado es ZIP: '{os.path.basename(downloaded_file)}'. Intentando descomprimir...")
                                    extracted_pdfs = unzip_pdfs(downloaded_file, download_dir)
                                    if extracted_pdfs:
                                        downloaded_paths_for_rd.extend(extracted_pdfs)
                                    else:
                                        print(f"         ⚠️ No se encontraron PDFs válidos dentro del ZIP o falló la descompresión. Se mantendrá el ZIP si no se eliminó.")
                                        # Si el ZIP no contenía PDFs o falló, puedes optar por guardar la ruta del ZIP si es relevante,
                                        # o registrar que no hubo PDFs. Por ahora, si no hay PDFs extraídos, no se añade ruta.
                                else:
                                    # Si es un PDF directo o cualquier otro archivo que queremos guardar tal cual
                                    downloaded_paths_for_rd.append(downloaded_file)
                                    print(f"         ✅ Archivo descargado directamente: '{os.path.basename(downloaded_file)}'")
                            else:
                                print(f"         ⚠️ Descarga FALLIDA o archivo no encontrado después de intento para el enlace de '{pdf_name_from_text}'. No se añadirá ruta.")


                    if found_pdf_names:
                        resoluciones_df.loc[original_mongo_index, 'PDF-name'] = " - ".join(found_pdf_names)
                        print(f"   ✅ 'PDF-name' actualizado para '{rd_value}': '{resoluciones_df.loc[original_mongo_index, 'PDF-name']}'")
                    
                    if downloaded_paths_for_rd:
                        # Unir las rutas de PDFs (pueden ser múltiples del ZIP)
                        resoluciones_df.loc[original_mongo_index, 'PDF-Path'] = " - ".join(downloaded_paths_for_rd)
                        print(f"   📂 Rutas PDF descargadas para '{rd_value}': {', '.join(downloaded_paths_for_rd)}")
                    else:
                        resoluciones_df.loc[original_mongo_index, 'PDF-Path'] = "NO DATA"
                        print(f"   ⚠️ No se pudieron descargar PDFs (o extraer de ZIP) para '{rd_value}'. 'PDF-Path' se mantiene como 'NO DATA'.")

                else:
                    print("      No se encontraron enlaces PDF/ZIP para esta RD en la tabla.")
                    print(f"   ⚠️ No se encontraron resultados o enlaces (PDF/ZIP) para '{rd_value}'. 'PDF-name' y 'PDF-Path' se mantienen como 'NO DATA'.")


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
                    # Re-inicializar selectores después de recargar la página
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
        
        for index, row in resoluciones_df.iterrows():
            res_val = row['Resolución']
            match_indices = final_df[final_df['Resolución'] == res_val].index
            if not match_indices.empty:
                final_df.loc[match_indices, 'PDF-name'] = row['PDF-name']
                final_df.loc[match_indices, 'PDF-Path'] = row['PDF-Path']
            else:
                pass 
        
        final_df.to_excel(output_excel_path, sheet_name="Data", index=False)
        print(f"\n✅ DataFrame FINAL exportado con éxito a: {output_excel_path}")
    except PermissionError as e:
        print(f"❌ Error de Permiso al exportar el DataFrame a Excel: {e}")
        print("Por favor, asegúrate de que el archivo no esté abierto y que tienes permisos de escritura.")
    except Exception as e:
        print(f"❌ Error inesperado al exportar el DataFrame a Excel: {e}")