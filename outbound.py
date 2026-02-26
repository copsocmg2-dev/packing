import json
import os
import time
import logging
from datetime import datetime, timedelta
import pytz

# --- Importações do Selenium ---
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Importações do Google Sheets (Service Account) ---
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# =================================================================
# CONFIGURAÇÃO GERAL
# =================================================================

# --- Credenciais Shopee (Via Variáveis de Ambiente) ---
SPX_USERNAME = os.environ.get("SPX_USERNAME", "Ops107156")
SPX_PASSWORD = os.environ.get("SPX_PASSWORD", "SuaSenhaAqui") # Configure no Secrets do GitHub

STAGING_SPREADSHEET_ID = "16hmhjLLu-TNIa17gLLBPN_IGvymdO7lL1yUn3NlrdKg"

# Nomes das Abas e Configurações Google
STAGING_SHEET_NAME = "staging_area_data"
PACKED_SHEET_NAME = "tos_packed"
SERVICE_ACCOUNT_FILE = "service.json"

# Configurações de Execução
TIMEZONE = "America/Sao_Paulo"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# Configuração de Dias para olhar para trás
DAYS_TO_FETCH = 7

# --- URLs das APIs ---
API_STAGING_SEARCH = "https://spx.shopee.com.br/api/in-station/outbound/outbound_staging_area/config/search"
API_STAGING_DETAIL = "https://spx.shopee.com.br/api/in-station/outbound/outbound_staging_area/details"
API_CAGE_DETAIL_LIST = "https://spx.shopee.com.br/api/in-station/cage/detail/list"
API_TO_DETAIL = "https://spx.shopee.com.br/api/in-station/general_to/detail/search"
API_PACKED_SEARCH = "https://spx.shopee.com.br/api/in-station/general_to/outbound/search"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# =================================================================
# LÓGICA DE API (VIA NAVEGADOR)
# =================================================================

def executar_api_via_browser(driver, method, url, referer, payload=None):
    try:
        if "shopee.com.br" not in driver.current_url:
            driver.get("https://spx.shopee.com.br/dashboard/overview")
            time.sleep(2)

        body_js = f"body: JSON.stringify({json.dumps(payload)})," if payload else ""
        method_js = method.upper()

        script = f"""
        var callback = arguments[arguments.length - 1];
        var csrfToken = (document.cookie.match(/csrftoken=([^;]+)/) || [])[1];
        
        fetch('{url}', {{
            method: '{method_js}',
            credentials: 'include',
            {body_js}
            headers: {{
                'Content-Type': 'application/json',
                'App': 'FMS Portal',
                'x-csrftoken': csrfToken,
                'Accept': 'application/json, text/plain, */*'
            }}
        }})
        .then(response => response.json())
        .then(data => callback(data))
        .catch(error => callback({{ 'error': error.toString() }}));
        """
        
        json_response = driver.execute_async_script(script)

        if not json_response:
            return None
        
        if 'error' in json_response:
            logging.error(f"Erro JS Fetch: {json_response['error']}")
            return None

        if json_response.get("retcode") != 0:
            msg_erro = str(json_response.get('message', '')).lower()
            if "not found" not in msg_erro and "incorreto" not in msg_erro:
                logging.error(f"API Retorno ({url}): {json_response.get('message')}")
            return None

        return json_response.get("data")

    except Exception as e:
        logging.error(f"Falha ao executar script no browser: {e}")
        return None

# =================================================================
# LÓGICA: STAGING AREA
# =================================================================

def consultar_detalhes_to_rapido(driver, to_number):
    url = f"{API_TO_DETAIL}?to_number={to_number}&pageno=1&count=10"
    referer = "https://spx.shopee.com.br/in-station/transfer-order"
    data = executar_api_via_browser(driver, 'GET', url, referer)
    if data:
        return data.get('pack_name', 'N/A'), data.get('quantity', 0), data.get('weight', 0)
    return "N/A", 0, 0

def consultar_itens_gaiola_rapido(driver, cage_id):
    payload = {"cage_id": cage_id, "pageno": 1, "page_size": 200}
    referer = f"https://spx.shopee.com.br/in-station/cage/detail/{cage_id}"
    data = executar_api_via_browser(driver, 'POST', API_CAGE_DETAIL_LIST, referer, payload)
    
    lista_tos = []
    if data and data.get("list"):
        for item in data["list"]:
            lista_tos.append({
                'to_number': item.get('entity_id'),
                'quantity': item.get('parcel_quantity', 0),
                'scan_time': item.get('scan_time'),
                'weight': item.get('weight', 0) 
            })
    return lista_tos

def processar_todas_areas(driver, areas_list):
    resultados_totais = []
    
    for i, area in enumerate(areas_list):
        if i > 0 and i % 30 == 0:
            logging.info(f"--- LIMPEZA PREVENTIVA DE MEMÓRIA (Item {i}) ---")
            try:
                driver.get("about:blank")
                time.sleep(2)
                driver.get("https://spx.shopee.com.br/dashboard/overview")
                time.sleep(5)
                if "login" in driver.current_url.lower():
                    logging.warning("Sessão caiu durante a limpeza. Refazendo login...")
                    login_shopee(driver)
                    time.sleep(3)
            except Exception as e:
                logging.error(f"Erro ao limpar memória: {e}")

        area_id = area.get('staging_area_id')
        area_name = area.get('staging_area_name')
        logging.info(f"Processando Area {i+1}/{len(areas_list)}: {area_name}")
        
        referer_detail = f"https://spx.shopee.com.br/staging-area-management/outbound/detail/{area_id}"
        detail_pageno = 1
        
        while True:
            url_detail = f"{API_STAGING_DETAIL}?pageno={detail_pageno}&count=100&staging_area_id={area_id}"
            data_detail = executar_api_via_browser(driver, 'GET', url_detail, referer_detail)
            
            if not data_detail: break
            items_list = data_detail.get("staging_area_item", {}).get("list", [])
            if not items_list: break
            
            for item in items_list:
                target_number = item.get('target_item_number', '')
                to_receiver = item.get('to_receiver', '')
                scan_time = item.get('scan_time') 
                item_type = item.get('target_item_type')

                if item_type == 7 or target_number.startswith(("CG", "SC")):
                    cage_id = target_number
                    tos_na_gaiola = consultar_itens_gaiola_rapido(driver, cage_id)
                    
                    if not tos_na_gaiola:
                        resultados_totais.append([area_id, area_name, "", cage_id, to_receiver, scan_time, "Vazio/Erro", 0, 0])
                    else:
                        for to_obj in tos_na_gaiola:
                            p_name, p_qty, p_weight = consultar_detalhes_to_rapido(driver, to_obj['to_number'])
                            qty_final = to_obj['quantity'] if to_obj['quantity'] > 0 else p_qty
                            weight_final = p_weight if p_weight > 0 else to_obj['weight']
                            
                            resultados_totais.append([
                                area_id, area_name, to_obj['to_number'], cage_id, to_receiver, 
                                to_obj['scan_time'], p_name, qty_final, weight_final
                            ])
                else:
                    to_number = target_number
                    p_name, qty, p_weight = consultar_detalhes_to_rapido(driver, to_number)
                    resultados_totais.append([
                        area_id, area_name, to_number, "-", to_receiver, 
                        scan_time, p_name, qty, p_weight
                    ])
            
            if len(items_list) < 100: break
            detail_pageno += 1
            
    return resultados_totais

# =================================================================
# LÓGICA: TOs PACKED (DIA-A-DIA)
# =================================================================

def coletar_dados_packed(driver):
    logging.info(f"--- Iniciando Coleta: TOs Packed (Últimos {DAYS_TO_FETCH} dias) ---")
    
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    referer = "https://spx.shopee.com.br/general-to-management"
    
    dados_totais = []
    
    for i in range(DAYS_TO_FETCH):
        dia_alvo = now - timedelta(days=i)
        start_dt = dia_alvo.replace(hour=0, minute=0, second=0, microsecond=0)
        end_dt = dia_alvo.replace(hour=23, minute=59, second=59, microsecond=999999)
        
        ctime_param = f"{int(start_dt.timestamp())},{int(end_dt.timestamp())}"
        logging.info(f" -> Buscando dados de: {start_dt.strftime('%Y-%m-%d')}")
        
        pageno = 1
        count_dia = 0
        
        while True:
            url = f"{API_PACKED_SEARCH}?pageno={pageno}&count=100&status=2&ctime={ctime_param}"
            data = executar_api_via_browser(driver, 'GET', url, referer)
            
            if not data or not data.get("list"):
                break
                
            items = data.get("list", [])
            for item in items:
                row = [
                    item.get('to_number'),
                    item.get('pack_name'),       
                    item.get('quantity'),
                    item.get('status'),          
                    item.get('complete_time'),   
                    item.get('staging_area_id'),
                    item.get('receiver'),
                    item.get('weight')
                ]
                dados_totais.append(row)
                
            count_dia += len(items)
            if len(items) < 100: break
            pageno += 1
            
        logging.info(f"    Total encontrado em {start_dt.strftime('%d/%m')}: {count_dia} itens.")

    logging.info(f"Coleta Packed Finalizada. Total acumulado: {len(dados_totais)}")
    return dados_totais

# =================================================================
# GOOGLE SHEETS E UTILS
# =================================================================

def formatar_timestamp(ts):
    if not ts or ts == 0 or ts == "0": return ""
    try:
        tz = pytz.timezone(TIMEZONE)
        val = float(ts)
        if val > 3000000000: val /= 1000
        return datetime.fromtimestamp(val, tz).strftime('%Y-%m-%d %H:%M:%S')
    except: return str(ts)

def get_sheets_service():
    """Conecta ao Google Sheets usando a Service Account configurada no GitHub Actions"""
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        creds_env = os.environ.get("GOOGLE_CREDENTIALS_JSON")
        if creds_env:
            with open(SERVICE_ACCOUNT_FILE, "w") as f: 
                f.write(creds_env)
                
    if os.path.exists(SERVICE_ACCOUNT_FILE): 
        creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        return build("sheets", "v4", credentials=creds)
    else: 
        logging.error("Nenhuma credencial do Google encontrada nas variáveis de ambiente.")
        return None

def write_generic_sheet(service, sheet_name, header, data, timestamp_col_index=None):
    if not service:
        return
        
    try:
        final_data = []
        if data:
            for row in data:
                row_copy = list(row)
                if timestamp_col_index is not None and len(row_copy) > timestamp_col_index:
                    row_copy[timestamp_col_index] = formatar_timestamp(row_copy[timestamp_col_index])
                final_data.append(row_copy)

        body = {'values': [header] + final_data}
        service.spreadsheets().values().clear(spreadsheetId=STAGING_SPREADSHEET_ID, range=f"'{sheet_name}'").execute()
        
        if final_data or header:
            service.spreadsheets().values().update(
                spreadsheetId=STAGING_SPREADSHEET_ID, range=f"'{sheet_name}'!A1", 
                valueInputOption="USER_ENTERED", body=body).execute()
        
        ts = datetime.now(pytz.timezone(TIMEZONE)).strftime('%Y-%m-%d %H:%M:%S')
        service.spreadsheets().values().append(
            spreadsheetId=STAGING_SPREADSHEET_ID, range=f"'{sheet_name}'!J1",
            valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS", body={'values': [["Última Atualização:", ts]]}
        ).execute()
        
        logging.info(f"Sucesso na aba '{sheet_name}': {len(final_data)} linhas.")
    except Exception as e:
        logging.error(f"Erro ao escrever na aba {sheet_name}: {e}")

# =================================================================
# MAIN E LOGIN
# =================================================================

def login_shopee(driver):
    driver.get("https://spx.shopee.com.br/")
    try:
        time.sleep(3)
        if "/login" not in driver.current_url: return True
        logging.info("Logando Shopee com Ops...")
        
        u = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, "//input[@type='text' or contains(@placeholder, 'mail')]")))
        u.click()
        u.send_keys(Keys.CONTROL + "a")
        u.send_keys(Keys.DELETE)
        u.send_keys(SPX_USERNAME)
        
        p = driver.find_element(By.XPATH, "//input[@type='password']")
        p.click()
        p.send_keys(Keys.CONTROL + "a")
        p.send_keys(Keys.DELETE)
        p.send_keys(SPX_PASSWORD)
        
        p.send_keys(Keys.ENTER)
        time.sleep(5)
        
        if "/login" not in driver.current_url:
            driver.get("https://spx.shopee.com.br/dashboard/overview")
            return True
    except Exception as e:
        logging.error(f"Erro durante login Shopee: {e}")
        
    return False

def login_selenium_inicial():
    logging.info("Iniciando Selenium...")
    options = webdriver.ChromeOptions()
    
    # Adicionando argumentos essenciais para rodar liso no GitHub Actions
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--log-level=3")
    
    try:
        # O Selenium 4 já acha e baixa o driver correto nativamente!
        driver = webdriver.Chrome(options=options)
        
        if login_shopee(driver):
            logging.info("Sessão ativa e pronta.")
            return driver
        else:
            logging.error("Falha ao logar. Verifique as credenciais.")
            driver.quit()
            return None
            
    except Exception as e:
        logging.error(f"Erro ao abrir driver: {e}")
        if 'driver' in locals() and driver: driver.quit()
        return None

def main():
    service = get_sheets_service()
    if not service:
        logging.error("Interrompendo a execução: Falha ao carregar Google Sheets via Service Account.")
        return
    
    driver = None
    try:
        driver = login_selenium_inicial()
        if not driver:
            logging.error("Falha ao abrir navegador. Encerrando execução.")
            return

        logging.info("### INICIANDO CICLO ÚNICO DE COLETA (GITHUB ACTIONS) ###")
        start_total = time.time()
        
        # --- PARTE 1: STAGING AREA ---
        logging.info(">> Etapa 1: Staging Area...")
        staging_header = ["ID Area", "Nome Area", "TO Number", "Cage ID", "Receiver", "Scan Time", "Pack Name", "Quantity", "Weight"]
        
        referer_list = "https://spx.shopee.com.br/staging-area-management/list/outbound"
        staging_areas = []
        pageno = 1
        
        while True:
            data = executar_api_via_browser(driver, 'POST', API_STAGING_SEARCH, referer_list, {"pageno": pageno, "count": 100})
            if not data or not data.get("list"): break
            staging_areas.extend(data.get("list"))
            if len(data.get("list")) < 100: break
            pageno += 1
        
        if not staging_areas:
            logging.warning("Nenhuma área encontrada ou erro na API.")
        else:
            dados_staging = processar_todas_areas(driver, staging_areas)
            write_generic_sheet(service, STAGING_SHEET_NAME, staging_header, dados_staging, timestamp_col_index=5)

        # --- PARTE 2: TOs PACKED ---
        logging.info(">> Etapa 2: TOs Packed...")
        packed_header = ["TO Number", "TO Pack", "Quantity", "Status", "Complete time", "Staging Area", "Receiver", "Weight"]
        dados_packed = coletar_dados_packed(driver)
        write_generic_sheet(service, PACKED_SHEET_NAME, packed_header, dados_packed, timestamp_col_index=4)
        
        duration = time.time() - start_total
        logging.info(f"Ciclo completo finalizado em {duration:.2f}s.")

    except Exception as e:
        logging.error(f"CRASH DETECTADO: {e}")
    finally:
        # É obrigatório matar o driver no GitHub Actions para não gerar travamentos (hanging)
        if driver:
            try:
                driver.quit()
                logging.info("Navegador encerrado.")
            except:
                pass

if __name__ == "__main__":
    main()
