from webdriver_manager.chrome import ChromeDriverManager
# Importa el controlador principal de Selenium para controlar el navegador (Chrome, Firefox, etc.)
from selenium import webdriver

# Permite definir el servicio de ChromeDriver (por ejemplo, la ruta del ejecutable)
from selenium.webdriver.chrome.service import Service

# Permite localizar elementos en la página por diferentes métodos: ID, clase, nombre, XPath, etc.
from selenium.webdriver.common.by import By

# Proporciona teclas del teclado (como ENTER, ESCAPE, etc.) para simular pulsaciones
from selenium.webdriver.common.keys import Keys

# Permite esperar de forma explícita hasta que se cumpla una condición (por ejemplo, que un elemento sea visible)
from selenium.webdriver.support.ui import WebDriverWait

# Permite interactuar con menús desplegables (`<select>`) como si se seleccionaran manualmente
from selenium.webdriver.support.ui import Select

# Define condiciones esperadas que se pueden usar con WebDriverWait (como presencia o visibilidad de elementos)
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd


yo = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

url_web = "https://consultasenlinea.mincetur.gob.pe/webCasinos/Index.aspx?po=frmSalas.aspx"

yo.get(url_web)

# Esperar hasta que aparezca la tabla dentro del div (máx 60 segundos)
WebDriverWait(yo, 60).until(
    EC.presence_of_element_located((By.CSS_SELECTOR, "#divResultadoSala table[cellspacing='0']")) 
)

# Lista para guardar los datos
todos_los_datos = []
pagina = 1

while True:
    print(f"📄 Extrayendo página {pagina}...")

    try:
        # Esperar a que la tabla esté presente
        WebDriverWait(yo, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "#divResultadoSala table[cellspacing='0']"))
        )

        # Obtener la tabla y sus filas
        tabla = yo.find_element(By.CSS_SELECTOR, "#divResultadoSala table[cellspacing='0']")
        filas = tabla.find_elements(By.XPATH, ".//tr[@data-id]")

        for fila in filas:
            celdas = fila.find_elements(By.TAG_NAME, "td")
            datos = [celda.text.strip() for celda in celdas]
            if datos:
                todos_los_datos.append(datos)

    except Exception as e:
        print("❌ No se encontró la tabla o ocurrió un error:", e)
        break

    # Intentar hacer clic en el botón "siguiente"
    try:
        boton_siguiente = WebDriverWait(yo, 5).until(
            EC.element_to_be_clickable((By.CLASS_NAME, "siguiente"))
        )
        boton_siguiente.click()
        #time.sleep(3)  # Pequeña espera para evitar fallos por animación/cambio de página
        pagina += 1
    except Exception:
        print("✅ Fin de la paginación o no se encontró el botón 'siguiente'.")
        break

# Cerrar navegador
yo.quit()

columnas = [
    "Ruc", "Empresa", "Establecimiento", "Giro", "Resolución",
    "Código Sala", "Vigencia", "Dirección", "Distrito", "Provincia", "Departamento"
]

df = pd.DataFrame(todos_los_datos, columns=columnas)
df.to_excel("datos_casinos_salas.xlsx", sheet_name="Data", index=False)
df.to_csv("datos_casinos_salas.csv", index=False, encoding='utf-8-sig')

print(f"\n✅ Total de filas extraídas: {len(df)}")

print(df)