import os
import re
import time
import shutil
import zipfile
import configparser
import requests
import pandas as pd
from urllib.parse import unquote
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager

from database.db_connections import get_db  # Asegúrate que esta función esté disponible

def get_unique_path(path):
    base, ext = os.path.splitext(path)
    counter = 1
    while os.path.exists(path):
        path = f"{base}_{counter}{ext}"
        counter += 1
    return path

def unzip_and_collect_pdfs(zip_path, extract_to):
    pdfs = []
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        pdf_names = [f for f in zip_ref.namelist() if f.lower().endswith('.pdf')]
        print(f"📦 ZIP encontrado: {os.path.basename(zip_path)} con {len(pdf_names)} PDF(s)")
        for member in pdf_names:
            filename = os.path.basename(member)
            target_path = os.path.join(extract_to, filename)
            counter = 1
            while os.path.exists(target_path):
                base, ext = os.path.splitext(filename)
                filename = f"{base}_{counter}{ext}"
                target_path = os.path.join(extract_to, filename)
                counter += 1
            with zip_ref.open(member) as source, open(target_path, 'wb') as target:
                shutil.copyfileobj(source, target)
            pdfs.append(target_path)
            print(f"    ✅ Extraído: {filename}")
    os.remove(zip_path)
    return pdfs

def wait_for_download(download_dir, timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        files = [f for f in os.listdir(download_dir) if not f.endswith(('.crdownload', '.tmp'))]
        if files:
            full_paths = [os.path.join(download_dir, f) for f in files]
            if all(os.path.exists(p) for p in full_paths):
                return full_paths
        time.sleep(1)
    return []

def scrape_resoluciones_from_df():
    config = configparser.ConfigParser()
    config.read('configs/settings.ini')

    url = config['SCRAPING_PDF']['CASINOS_PDF_URL']
    collection_name = config['MONGO']['COLLECTION_LOAD']
    output_path = config['DATA_OUTPUT']['PROCESSED_PATH']
    download_dir = config['DATA_OUTPUT']['DOWNLOAD_PATH']

    os.makedirs(output_path, exist_ok=True)
    os.makedirs(download_dir, exist_ok=True)
    excel_path = os.path.join(output_path, "data_casinos_pdf.xlsx")

    db = get_db()
    if db is None:
        print("❌ No hay conexión a la base de datos")
        return

    df = pd.DataFrame(list(db[collection_name].find({}, {"_id": 0})))
    if df.empty:
        df = pd.DataFrame(columns=["Resolución", "Anio", "PDF-name", "PDF-Path"])
        df.to_excel(excel_path, index=False)
        return

    df['Resolución'] = df['Resolución'].astype(str).str.strip()
    split_data = df['Resolución'].str.rsplit('-', n=1, expand=True)
    df['Anio'] = split_data[1].where(split_data[1].str.match(r'^\d{4}$'))
    df['Resolución'] = split_data[0]
    df['PDF-name'] = "NO DATA"
    df['PDF-Path'] = "NO DATA"

    options = Options()
    prefs = {"download.default_directory": os.path.abspath(download_dir),
             "download.prompt_for_download": False,
             "plugins.always_open_pdf_externally": True}
    options.add_experimental_option("prefs", prefs)

    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
    driver.get(url)

    def init_page():
        Select(WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, 'UC_TIP_DISP_OBJ_DDL_COD_DISP')))).select_by_value("5")
        Select(WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, 'UC_ANO_OBJ_DDL_ANO_PROC')))).select_by_visible_text("--Todos--")

    init_page()

    for i, row in df.iterrows():
        try:
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, 'TB_NRO_RESO'))).clear()
            driver.find_element(By.ID, 'TB_NRO_RESO').send_keys(row['Resolución'])
            driver.find_element(By.ID, 'BUSCAR').click()

            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'trans_td')))
            links = driver.find_elements(By.XPATH, '//a[contains(@href, "Imagen.aspx")]')

            file_names, file_paths = [], []
            for idx, link in enumerate(links):
                name = link.text.strip()
                extension = ".pdf"  # Asumimos PDF para renombrar correctamente
                safe_name = re.sub(r'[\\/:*?"<>|]', '_', name)
                unique_name = f"{safe_name}_{idx + 1}{extension}" if len(links) > 1 else f"{safe_name}{extension}"
                renamed_path = get_unique_path(os.path.join(download_dir, unique_name))
                print(f"📥 Esperando descarga para: {os.path.basename(renamed_path)}")
                link.click()
                files = wait_for_download(download_dir, timeout=60)
                for file in files:
                    if not os.path.exists(file):
                        continue
                    if os.path.isdir(file):
                        continue
                    if file == renamed_path:
                        continue
                    if not file.lower().endswith(('.pdf', '.zip')):
                        continue

                    os.rename(file, renamed_path)

                    if renamed_path.endswith('.zip'):
                        pdfs = unzip_and_collect_pdfs(renamed_path, download_dir)
                        file_paths.extend(pdfs)
                        file_names.extend([os.path.basename(p) for p in pdfs])
                    else:
                        file_paths.append(renamed_path)
                        file_names.append(os.path.basename(renamed_path))

            if file_names:
                df.at[i, 'PDF-name'] = ', '.join(file_names)
                df.at[i, 'PDF-Path'] = ', '.join(file_paths)

            df.to_excel(excel_path, index=False)
        except Exception as e:
            print(f"Error en resolución {row['Resolución']}: {e}")
            driver.get(url)
            init_page()

    driver.quit()
    df.to_excel(excel_path, index=False)
    print(f"✅ Proceso finalizado. Archivo guardado en: {excel_path}")
