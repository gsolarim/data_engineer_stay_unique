# ======================= Importación de Librerías =======================
import csv
import json
import random
import re
import time

import pandas as pd

from IPython.display import Image  # Mostrar imágenes en entornos interactivos (opcional)
from selenium import webdriver  # Automatización del navegador
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager  # Gestor automático de drivers

from etl_bookings_properties import upload_to_bigquery

# ======================= Configuración del Navegador =======================
options = Options()
options.add_argument('--headless')  # Ejecutar en modo sin interfaz gráfica
options.add_argument('--no-sandbox')  # Evitar errores de usuario en entornos restringidos
options.add_argument('--disable-dev-shm-usage')  # Incrementa memoria compartida en contenedores

# ======================= Inicialización del Navegador =======================
browser = webdriver.Chrome(
    service=Service(ChromeDriverManager().install()), 
    options=options
)
browser.maximize_window()  # Maximizar la ventana del navegador al abrir


# ======================= Función para Mostrar Captura de Pantalla =======================
def show_screenshot(browser, fuente):
    """
    Guarda una captura de pantalla del navegador y la muestra.

    Parameters:
    - browser (webdriver): instancia del navegador Selenium
    - fuente (str): nombre de la fuente, usado para el archivo de imagen

    Returns:
    - Image: visualización de la captura de pantalla en el entorno interactivo
    """
    # Definir el nombre del archivo y la ruta donde se guardará
    path_png = f'./imgs/{fuente}.png'
    
    # Guardar la captura de pantalla en el archivo especificado
    browser.save_screenshot(path_png)
    
    # Mostrar la captura de pantalla guardada
    return Image(path_png)

# ======================= Función para Extraer el Precio =======================
def extract_price(text):
    """
    Extrae el valor numérico de un texto que contiene el símbolo de la moneda 'S/' seguido de un número.

    Parameters:
    - text (str): cadena de texto que contiene el precio en formato "S/ <número>"

    Returns:
    - str: el número extraído como cadena si se encuentra el patrón
    - None: si no se encuentra el patrón de precio en el texto
    """
    # Buscar el patrón "S/" seguido de un número
    match = re.search(r'S/\s*(\d+)', text)
    
    # Retornar solo el número si se encuentra coincidencia, de lo contrario, retornar None
    if match:
        return match.group(1)
    
    return None


# ======================= Función para Realizar Búsqueda =======================
def search_data(inp_search, browser):
    """
    Realiza una búsqueda en la página web usando el término de entrada en el campo de búsqueda.

    Parameters:
    - inp_search (str): término a buscar (ej. 'Barcelona')
    - browser (webdriver): instancia del navegador de Selenium

    Returns:
    - None
    """

    # Insertar el término de búsqueda en el campo correspondiente
    id_input = 'bigsearch-query-location-input'
    e_input = browser.find_element(By.ID, id_input)
    e_input.clear()  # Limpia el campo de entrada de búsqueda
    e_input.send_keys(inp_search)  # Escribe el término de búsqueda
    time.sleep(1)  # Pausa breve para estabilidad de la interfaz
    e_input.send_keys(Keys.RETURN)  # Envía la búsqueda

    # Hacer clic en el botón de búsqueda
    data_test_id_btn = "[data-testid='structured-search-input-search-button']"
    c_btn = browser.find_element(By.CSS_SELECTOR, data_test_id_btn)
    c_btn.click()
    time.sleep(3)  # Pausa para permitir que la página cargue resultados

    # Confirmación de búsqueda realizada
    print(f'==> Se buscaron los datos para {inp_search}')


# ======================= Función para Guardar Registros de la Página =======================
# ======================= Función para Guardar Registros de la Página =======================
def save_records_from_page(browser, list_dict):
    wait_time = 2
    # Definir el xpath para encontrar los registros de reservas
    xpath_result = '//div[@class="gsgwcjk atm_10yczz8_kb7nvz atm_10yczz8_cs5v99__1ldigyt atm_10yczz8_11wpgbn__1v156lz atm_10yczz8_egatvm__qky54b atm_10yczz8_qfx8er__1xolj55 atm_10yczz8_ouytup__w5e62l g8ge8f1 atm_1d13e1y_p5ox87 atm_yrukzc_1od0ugv atm_10yczz8_cs5v99_vagkz0_1ldigyt atm_10yczz8_11wpgbn_vagkz0_1h2hqoz g14v8520 atm_9s_11p5wf0 atm_d5_j5tqy atm_d7_1ymvx20 atm_dl_1mvrszh atm_dz_hxz02 dir dir-ltr"]/div'
    
    # Obtener los elementos de reservas mediante el xpath
    a_bookings = browser.find_elements(By.XPATH, xpath_result)

    for a_booking in a_bookings:
        # Inicializar un diccionario para almacenar los datos de cada reserva
        dict_data = dict()

        # Obtener el enlace de la reserva
        link_booking = a_booking.find_element(By.XPATH,'.//meta[@itemprop = "url"]')
        dict_data['link_booking'] = link_booking.get_attribute('content')

        # Obtener el nombre de la reserva
        name_booking = a_booking.find_element(By.XPATH, './/meta[@itemprop = "name"]')
        dict_data['name_booking'] = name_booking.get_attribute('content')

        # Definiendo nueva ventana
        browser.execute_script('window.open();')  # Abrir una nueva ventana
        window_before = browser.window_handles[0]  # Almacenar la ventana original
        window_after = browser.window_handles[-1]  # Almacenar la nueva ventana

        # Cambiando de ventana
        browser.switch_to.window(window_after)

        # Asegurando que la URL tenga https:// delante
        link_booking_url = dict_data['link_booking']
        if not link_booking_url.startswith(('http://', 'https://')):
            link_booking_url = 'https://' + link_booking_url  # Agregar https:// por defecto

        # Cargar la URL de la reserva
        browser.get(link_booking_url)
        time.sleep(random.uniform(1, 2))  # Espera un tiempo aleatorio

        # Definir el xpath para obtener el precio de la reserva
        xpath_precio = './/div[@data-section-id="BOOK_IT_SIDEBAR"]//span[1]'
        WebDriverWait(browser, wait_time).until(EC.presence_of_element_located((By.XPATH, xpath_precio)))
        time.sleep(2)  # Espera adicional para cargar el precio
        precio_booking = browser.find_elements(By.XPATH, xpath_precio)
        dict_data['precio_booking'] = extract_price(precio_booking[0].text)

        # Obtener la calificación de la reserva
        xpath_rating_1 = './/section//div/span[@class="a8jt5op atm_3f_idpfg4 atm_7h_hxbz6r atm_7i_ysn8ba atm_e2_t94yts atm_ks_zryt35 atm_l8_idpfg4 atm_mk_stnw88 atm_vv_1q9ccgz atm_vy_t94yts dir dir-ltr"]'
        rating_1_booking = browser.find_elements(By.XPATH, xpath_rating_1)
        dict_data['rating_1_booking'] = rating_1_booking[0].text if rating_1_booking else ''

        # Obtener una segunda calificación de la reserva
        xpath_rating_2 = './/div[@class="a8jhwcl atm_c8_vvn7el atm_g3_k2d186 atm_fr_1vi102y atm_9s_1txwivl atm_ar_1bp4okc atm_h_1h6ojuz atm_cx_t94yts atm_le_14y27yu atm_c8_sz6sci__14195v1 atm_g3_17zsb9a__14195v1 atm_fr_kzfbxz__14195v1 atm_cx_1l7b3ar__14195v1 atm_le_1l7b3ar__14195v1 dir dir-ltr"]'
        rating_2_booking = browser.find_elements(By.XPATH, xpath_rating_2)
        dict_data['rating_2_booking'] = rating_2_booking[0].text if rating_2_booking else ''

        # Determinar la calificación de la reserva
        if dict_data['rating_1_booking'][:12] == 'Calificación':
            dict_data['rating_booking'] = dict_data['rating_1_booking']
        else:
            dict_data['rating_booking'] = dict_data['rating_2_booking']

        # Extraer el valor numérico de la calificación
        extrae_rating = re.search(r'de (\d+\.\d+)', dict_data['rating_booking'])  
        if dict_data['rating_booking'] == '':
            dict_data['rating_booking'] = ''
        elif dict_data['rating_booking']:
            dict_data['rating_booking'] = extrae_rating.group(1)

        # Obtener la primera reseña de la reserva
        xpath_review_1 = './/a[@class="l1ovpqvx atm_1he2i46_1k8pnbi_10saat9 atm_yxpdqi_1pv6nv4_10saat9 atm_1a0hdzc_w1h1e8_10saat9 atm_2bu6ew_929bqk_10saat9 atm_12oyo1u_73u7pn_10saat9 atm_fiaz40_1etamxe_10saat9 b1uxatsa atm_c8_1kw7nm4 atm_bx_1kw7nm4 atm_cd_1kw7nm4 atm_ci_1kw7nm4 atm_g3_1kw7nm4 atm_9j_tlke0l_1nos8r_uv4tnr atm_7l_1kw7nm4_pfnrn2 atm_rd_8stvzk_pfnrn2 c1qih7tm atm_1s_glywfm atm_26_1j28jx2 atm_3f_idpfg4 atm_9j_tlke0l atm_gi_idpfg4 atm_l8_idpfg4 atm_vb_1wugsn5 atm_7l_jt7fhx atm_rd_8stvzk atm_5j_1896hn4 atm_cs_10d11i2 atm_r3_1kw7nm4 atm_mk_h2mmj6 atm_kd_glywfm atm_9j_13gfvf7_1o5j5ji atm_7l_jt7fhx_v5whe7 atm_rd_8stvzk_v5whe7 atm_7l_177r58q_1nos8r_uv4tnr atm_rd_8stvzk_1nos8r_uv4tnr atm_7l_9vytuy_4fughm_uv4tnr atm_rd_8stvzk_4fughm_uv4tnr atm_rd_8stvzk_xggcrc_uv4tnr atm_7l_1he744i_csw3t1 atm_rd_8stvzk_csw3t1 atm_3f_glywfm_jo46a5 atm_l8_idpfg4_jo46a5 atm_gi_idpfg4_jo46a5 atm_3f_glywfm_1icshfk atm_kd_glywfm_19774hq atm_7l_jt7fhx_1w3cfyq atm_rd_8stvzk_1w3cfyq atm_uc_aaiy6o_1w3cfyq atm_70_1p56tq7_1w3cfyq atm_uc_glywfm_1w3cfyq_1rrf6b5 atm_7l_jt7fhx_pfnrn2_1oszvuo atm_rd_8stvzk_pfnrn2_1oszvuo atm_uc_aaiy6o_pfnrn2_1oszvuo atm_70_1p56tq7_pfnrn2_1oszvuo atm_uc_glywfm_pfnrn2_1o31aam atm_7l_9vytuy_1o5j5ji atm_rd_8stvzk_1o5j5ji atm_rd_8stvzk_1mj13j2 dir dir-ltr"]'
        review_1_booking = browser.find_elements(By.XPATH, xpath_review_1)
        dict_data['review_1_booking'] = review_1_booking[0].text if review_1_booking else ''

        # Obtener la segunda reseña de la reserva
        xpath_review_2 = './/div[@class="rddb4xa atm_9s_1txwivl atm_ar_1bp4okc atm_h_1h6ojuz atm_cx_t94yts atm_le_yh40bf atm_le_idpfg4__14195v1 atm_cx_idpfg4__14195v1 dir dir-ltr"]'
        review_2_booking = browser.find_elements(By.XPATH, xpath_review_2)
        dict_data['review_2_booking'] = review_2_booking[0].text if review_2_booking else ''

        # Determinar la reseña principal de la reserva
        if "reseña" in dict_data['review_1_booking']:
            dict_data['review_booking'] = dict_data['review_1_booking']
        else:
            dict_data['review_booking'] = dict_data['review_2_booking']

        # Extraer el número de reseñas
        extrae_review = re.search(r'(\d+)\s*reseñas?', dict_data['review_booking'], re.IGNORECASE)
        if dict_data['review_booking'] == '':
            dict_data['review_booking'] = ''
        elif dict_data['review_booking']:
            dict_data['review_booking'] = extrae_review.group(1)

        # Eliminar claves innecesarias del diccionario
        keys_to_remove = {'rating_1_booking', 'rating_2_booking', 'review_1_booking', 'review_2_booking'}
        for key in keys_to_remove:
            dict_data.pop(key, None)  # None evita un error si la clave no existe

        # Cerrando pestaña actual
        browser.close()

        # Regresando a pestaña inicial
        browser.switch_to.window(window_before)

        # Agregar los datos de la reserva a la lista
        list_dict.append(dict_data)

    return list_dict


# ================== Función para Guardar Registros de Todas las Páginas ==================
def save_records_from_every_page(browser, list_dict):
    """
    Extrae y guarda registros de múltiples páginas hasta un máximo de 100 registros.

    Parameters:
    - browser (webdriver): instancia del navegador de Selenium
    - list_dict (list): lista de diccionarios para almacenar los datos de cada página

    Returns:
    - list_dict (list): lista de diccionarios actualizada con los registros de todas las páginas
    """

    page = 0  # Contador de páginas procesadas

    # Iterar mientras la cantidad de registros sea menor o igual a 100
    while len(list_dict) <= 100:
        page += 1

        # Guardar los registros de la página actual
        save_records_from_page(browser, list_dict)
        
        # XPath del botón de siguiente página
        xpath_next_page = '/a[@aria-label="Siguiente"]'
        go_next = browser.find_elements(By.XPATH, xpath_next_page)

        # Navegar a la siguiente página si el botón existe
        if go_next:
            browser.execute_script('arguments[0].click()', go_next[0])
        
        print(f'==> Se ha extraído la información de la página {page}')
    
    return list_dict


# ================= Guardar los Resultados en Formato CSV o JSON =================
def write_data(list_dict, fuente, filetype):
    """
    Guarda los datos en el formato especificado (CSV o JSON) en una ubicación determinada.

    Parameters:
    - list_dict (list): lista de diccionarios con los datos a guardar
    - fuente (str): nombre de la fuente que se usará para el nombre del archivo
    - filetype (str): tipo de archivo para guardar los datos ('csv' o 'json')

    Returns:
    - None
    """
    
    # Definir la ruta del archivo
    path_fname = f'./web_scraping_outputs/{fuente}.{filetype}'

    # Guardar los datos en formato CSV
    if filetype == 'csv':
        with open(path_fname, 'w', newline='') as f:
            writer = csv.DictWriter(f, delimiter=',', quotechar='"', fieldnames=list_dict[0].keys())
            writer.writeheader()
            writer.writerows(list_dict[:100])

    # Guardar los datos en formato JSON
    elif filetype == 'json':
        with open(path_fname, 'w') as f:
            json.dump(list_dict[:100], f)

    print(f'==> Los datos fueron guardados en formato {filetype}')


# ============================ Ejecución Principal ===============================
if __name__ == "__main__":
    # 1. Parámetros
    fuente = 'airbnb_barcelona'
    inp_search = 'Barcelona'
    url_root = 'https://www.airbnb.com/'
    filetype = 'csv'
    list_dict = []

    # 2. Navegar a https://www.airbnb.com/
    browser.get(url_root)

    # 3. Ejecutar las funciones principales
    time.sleep(1)
    search_data(inp_search, browser)
    save_records_from_every_page(browser, list_dict)
    write_data(list_dict, fuente, filetype)
    df = pd.DataFrame(list_dict)
    upload_to_bigquery(df, 'esoteric-throne-425604-u5', 'dataset_stayunique', 'bookings_scraping')

    # Cerrar el navegador
    browser.quit()