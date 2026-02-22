import json
import os
import time
import logging
import math
import gc
from datetime import datetime, timedelta
import pytz

# --- Importações do Selenium e Webdriver Manager ---
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# --- Importações do Google Sheets ---
from google.oauth2 import service_account
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# =================================================================
# CONFIGURAÇÕES
# =================================================================

# --- Credenciais ---
SPX_USERNAME = os.environ.get("SPX_USERNAME", "Ops107156")
SPX_PASSWORD = os.environ.get("SPX_PASSWORD", "Du96574892*")
ILOX_EMAIL = os.environ.get("ILOX_EMAIL", "joao.franco@shopee.com")
ILOX_SENHA = os.environ.get("ILOX_SENHA", "123")

# --- ID DA PLANILHA DE TESTE (CENTRALIZADA) ---
MAIN_SPREADSHEET_ID = "1wduCsbPhCCcGzqhugm8zeSAPui-_ibZzJBP2jBkwGQ4"

# --- Nomes das Abas ---
# Shopee
PRODUTIVIDADE_SHEET_NAME = "raw_spx_workstation"
OUTBOUND_SHEET_NAME = "raw_spx_packing_formated"
OUTBOUND_ORIGINAL_SHEET_NAME = "raw_spx_packing"
DOCK_QUEUE_SHEET_NAME = "raw_spx_dock_queue"
QUEUE_LOG_SHEET_NAME = "queue-list-log"
HISTORY_SHEET_NAME = "db_ended"
ALL_TRIPS_SHEET_NAME = "db_all"

# Ilox
HOURLY_DB_ONTEM_SHEET_NAME = "hourly_db_ontem" 
HISTORICO_PROD_SHEET_NAME = "historico_Prod"   
ILOX_HOURLY_SHEET_NAME = "hourly_db"           

# --- Configurações Gerais ---
TIMEZONE = "America/Sao_Paulo"
SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# --- URLs (Endpoints da API) ---
PRODUCTIVITY_API_URL = "https://spx.shopee.com.br/api/wfm/admin/workstation/productivity/productivity_individual_list"
OUTBOUND_API_URL = "https://spx.shopee.com.br/api/wfm/admin/dashboard/list"
DOCK_QUEUE_API_URL = "https://spx.shopee.com.br/api/in-station/dock_management/queue/list"
QUEUE_LOG_API_URL = "https://spx.shopee.com.br/api/in-station/dock_management/queue/log/list"
HISTORY_API_URL = "https://spx.shopee.com.br/api/admin/transportation/trip/history/list"
PENDING_TRIPS_API_URL = "https://spx.shopee.com.br/api/admin/transportation/trip/list_v2"
DEPARTED_TRIPS_API_URL = "https://spx.shopee.com.br/api/admin/transportation/trip/list"

# Ilox URLs
ILOX_DASHBOARD_URL = "https://iloxconnect.com/dashboard.php"
ILOX_API_DASHBOARD = "https://iloxconnect.com/api/apiDashboardData.php"
ILOX_API_HOURLY_PROD = "https://iloxconnect.com/api/apiHourlyProduction.php"
ILOX_API_HOURLY_REJ = "https://iloxconnect.com/api/apiHourlyRejection.php"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# =================================================================
# UTILITÁRIOS
# =================================================================

def matar_processos_presos():
    try:
        os.system("taskkill /F /IM chromedriver.exe /T >nul 2>&1")
        os.system("taskkill /F /IM chrome.exe /T >nul 2>&1")
    except: pass

def executar_chamada_api(driver, method, url, referer, payload=None):
    try:
        domain = "shopee.com.br" if "shopee" in url else "iloxconnect.com"
        if domain not in driver.current_url:
            driver.get(referer)
            time.sleep(2)
        
        headers = {'Referer': referer, 'Content-Type': 'application/json'}
        if "shopee" in url:
            headers['App'] = 'FMS Portal'
            token = None
            try:
                csrf = driver.get_cookie('csrftoken')
                if csrf: token = csrf['value']
            except: pass
            
            if not token:
                token = driver.execute_script("return (document.cookie.match(/csrftoken=([^;]+)/) || [])[1];")
            
            if token: headers['x-csrftoken'] = token
        
        fetch_opts = f"headers: {json.dumps(headers)}"
        if "ilox" in url: fetch_opts += ", credentials: 'include'"

        if method.upper() == 'POST':
            script = f"return await fetch('{url}', {{method: 'POST', {fetch_opts}, body: JSON.stringify({json.dumps(payload)}) }}).then(res => res.json());"
        else:
            script = f"return await fetch('{url}', {{method: 'GET', {fetch_opts} }}).then(res => res.json());"
            
        json_response = driver.execute_script(script)
        
        if json_response and isinstance(json_response, dict) and json_response.get("retcode", 0) != 0:
            return None
        return json_response.get("data") if "shopee" in url else json_response
    except Exception as e:
        logging.error(f"Erro request ({url}): {e}")
        return None

def formatar_tempo_de_espera(minutos):
    if not isinstance(minutos, (int, float)) or minutos <= 0: return "00:00"
    return f"{int(minutos // 60):02d}:{int(minutos % 60):02d}"

def mapear_status_doca(status_id):
    mapa = {1: "Pending", 2: "Assigned", 3: "Occupied", 4: "Ended", 5: "On Hold"}
    return mapa.get(status_id, str(status_id))

def mapear_tipo_chegada(tipo_id):
    mapa = {1: "Line Haul", 7: "First Mile", 3: "Returns"}
    return mapa.get(tipo_id, str(tipo_id))

def mapear_status_db_all(status_id, tipo='viagem'):
    mapas = {'viagem': {5: "Assigned", 10: "Em Andamento", 30:"Em Trânsito", 40: "Unseal", 50: "Arrived", 60: "Unseal(?)", 90: "Concluído"}, 'parada': {0: "Pending", 5: "Assigned", 10: "Loading", 30: "Em Trânsito(?)", 40: "Unseal", 50: "Arrived", 60: "Unseal", 80: "Unloaded", 90: "Concluído"}}
    try: return mapas[tipo].get(int(status_id), str(status_id))
    except: return str(status_id)

def traduzir_indicador_ontime(indicador_en):
    mapa = {"Late Arrival": "Chegada Atrasada", "Early Arrival": "Chegada Adiantada", "Waiting": "Aguardando", "Early Departure": "Partida Adiantada", "Late Departure": "Partida Atrasada", "On-Time Arrival": "Chegada Pontual", "Potentially Late": "Potencialmente Atrasado", "Potentially Early": "Potencialmente Adiantado"}
    return mapa.get(indicador_en, indicador_en) if indicador_en else ""

def formatar_timestamp_unix(ts):
    if not ts: return ""
    try: return datetime.fromtimestamp(float(ts), pytz.timezone(TIMEZONE)).strftime('%Y-%m-%d %H:%M:%S')
    except: return str(ts)

def formatar_timestamp_trips(ts, tz):
    if not ts: return ""
    try:
        ts_float = float(ts)
        if ts_float > 3000000000: ts_float /= 1000
        return datetime.fromtimestamp(ts_float, tz).strftime('%Y-%m-%d %H:%M:%S')
    except: return ""

def formatar_docks(dock_list):
    if not isinstance(dock_list, list): return ""
    return ", ".join([d.get('dock_name', 'N/A') for d in dock_list if isinstance(d, dict)])

def determinar_turno(timestamp, tz): 
    if not timestamp or timestamp == 0: return ""
    try:
        ts_float = float(timestamp)
        if ts_float > 3000000000: ts_float = ts_float / 1000
        dt = datetime.fromtimestamp(ts_float, tz)
        if 6 <= dt.hour <= 13: return "T1"
        elif 14 <= dt.hour <= 21: return "T2"
        else: return "T3"
    except: return ""

def calcular_periodos_coleta():
    tz = pytz.timezone(TIMEZONE)
    agora = datetime.now(tz)
    dia_trab = agora if agora.hour >= 6 else agora - timedelta(days=1)
    inicio = dia_trab.replace(hour=6, minute=0, second=0, microsecond=0)
    fim = (agora + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    periodos = []
    curr = inicio
    while curr < fim:
        prox = curr + timedelta(hours=1)
        periodos.append({'data_cal': curr.strftime('%Y-%m-%d'), 'data_trab': (curr if curr.hour >= 6 else curr - timedelta(days=1)).strftime('%Y-%m-%d'), 'inicio': curr.strftime('%H:%M'), 'fim': prox.strftime('%H:%M'), 'label': f"{curr.hour}-{prox.hour}"})
        curr = prox
    return periodos

# =================================================================
# FUNÇÕES DE COLETA SHOPEE
# =================================================================

def coletar_shopee_produtividade(driver):
    """Tempo Real"""
    logging.info("--- [SPX] Produtividade (Tempo Real) ---")
    periodos = calcular_periodos_coleta()
    dados = []
    tz = pytz.timezone(TIMEZONE)
    for p in periodos:
        dt_ini = tz.localize(datetime.strptime(f"{p['data_cal']} {p['inicio']}", '%Y-%m-%d %H:%M'))
        dt_fim = tz.localize(datetime.strptime(f"{p['data_cal']} {p['fim']}", '%Y-%m-%d %H:%M'))
        if dt_fim <= dt_ini: dt_fim += timedelta(days=1)
        url = f"{PRODUCTIVITY_API_URL}?pageno=1&count=500&start_time={int(dt_ini.timestamp())}&end_time={int(dt_fim.timestamp())}&activity_type=12"
        res = executar_chamada_api(driver, 'GET', url, "https://spx.shopee.com.br/admin/workstation/productivity")
        if res and res.get("list"):
            for item in res["list"]:
                ops = item.get("ops", "")
                ops_id, ops_name = (ops.split(']')[0].replace('[','').strip(), ops.split(']')[1].strip()) if ']' in ops else ('', ops)
                dados.append([ops_id, ops_name, item.get("workstation"), item.get("activity_type"), item.get("working_hours", 0), item.get("total_throughput", 0), item.get("check_in_time"), item.get("check_out_time"), '','', p['label'].split('-')[0], p['data_trab']])
    return dados

def coletar_shopee_outbound(driver):
    logging.info("--- [SPX] Outbound ---")
    payload = {"unit_type": 1, "process_type": 2, "period_type": 1, "pageno": 1, "count": 500, "productivity": 1, "order_by_total": 100, "event_id_list": []}
    res = executar_chamada_api(driver, 'POST', OUTBOUND_API_URL, "https://spx.shopee.com.br/dashboard/overview", payload)
    raw, fmt = [], []
    if res and res.get("efficiency_list"):
        h_atual = datetime.now(pytz.timezone(TIMEZONE)).hour
        for item in res["efficiency_list"]:
            eff = item.get("efficiency", []) + [0]*12
            eff = eff[:12]
            raw.append([item.get("operator", ""), item.get("efficiency_total", 0)] + eff)
            for i in range(12): fmt.append([item.get("operator", ""), item.get("efficiency_total", 0), (h_atual - i + 24) % 24, eff[i]])
    return raw, fmt

def coletar_shopee_dock(driver):
    logging.info("--- [SPX] Dock Queue ---")
    dados = []
    for status in ["1,2,3,5", "4"]:
        payload = {"pageno": 1, "count": 500, "queue_type": 1, "add_to_queue_time": "", "queue_status": status}
        res = executar_chamada_api(driver, 'POST', DOCK_QUEUE_API_URL, "https://spx.shopee.com.br/station/inbound/dock", payload)
        if res and res.get("list"):
            for i in res["list"]:
                dados.append([i.get('queue_number'), i.get('vehicle_number'), formatar_tempo_de_espera(i.get('waiting_time')), "Yes" if i.get('is_prioritized')==1 else "No", ','.join(map(str, i.get('prioritised_tags', []))), formatar_tempo_de_espera(i.get('on_hold_time')), i.get('route_info', {}).get('lh_trip_number'), i.get('route_info', {}).get('lh_trip_name'), i.get('handover_task_number'), i.get('order_quantity'), i.get('driver_name'), mapear_tipo_chegada(i.get('arrival_type')), i.get('agency'), "Yes" if i.get('is_printed') else "No", i.get('assigned_dock_name'), i.get('assigned_dock_group_name'), i.get('occupied_dock_name'), mapear_status_doca(i.get('queue_status')), i.get('occupancy_sequence'), formatar_timestamp_unix(i.get('add_to_queue_time'))])
    return dados

def coletar_shopee_db_all(driver):
    logging.info("--- [SPX] DB All ---")
    tz = pytz.timezone(TIMEZONE)
    def buscar_paginas(url_base, params_base, referer):
        itens, pag = [], 1
        while True:
            res = executar_chamada_api(driver, 'GET', f"{url_base}?{params_base}&pageno={pag}", referer)
            if not res or not res.get("list"): break
            itens.extend(res["list"])
            if len(res["list"]) < 50: break
            pag += 1
        return itens
    agora = datetime.now(tz)
    st, et = int(agora.replace(hour=0,minute=0).timestamp()), int((agora+timedelta(days=1)).replace(hour=23,minute=59).timestamp())
    pend = buscar_paginas(PENDING_TRIPS_API_URL, f"station_type=2,3,7,12,14,16,18&count=100&query_type=1&tab_type=3&std={st},{et}&trip_station_status=30,50,5,10", "https://spx.shopee.com.br/hubLinehaulTrips/trip")
    st_d = int((agora-timedelta(days=1)).replace(hour=0,minute=0).timestamp())
    dep = buscar_paginas(DEPARTED_TRIPS_API_URL, f"mtime={st_d},{et}&count=100&query_type=2", "https://spx.shopee.com.br/hubLinehaulTrips/trip")
    st_all = int((agora-timedelta(days=7)).replace(hour=0,minute=0).timestamp())
    all_tr = buscar_paginas(PENDING_TRIPS_API_URL, f"station_type=2,3,7,12,14,16,18&count=100&query_type=1&tab_type=1&display_range={st_all},{et}", "https://spx.shopee.com.br/hubLinehaulTrips/trip")
    combined, proc = [], set()
    def process(lst, is_dep=False):
        for i in lst:
            tid = i.get('trip_number')
            if tid in proc: continue
            stats = i.get('trip_station', [])
            if is_dep:
                if not any('SoC_MG_Betim' in s.get('station_name','') and s.get('trip_station_status')==40 for s in stats): continue
            for st in stats:
                if st.get('station_name') == 'SoC_MG_Betim':
                    combined.append([i.get('trip_number'), traduzir_indicador_ontime(st.get('on_time_indicator')), i.get('vehicle_type_name'), formatar_timestamp_trips(st.get('sta'), tz), formatar_timestamp_trips(st.get('std'), tz), formatar_timestamp_trips(st.get('ata'), tz), formatar_timestamp_trips(st.get('atd'), tz), formatar_timestamp_trips(st.get('eta'), tz), formatar_timestamp_trips(st.get('etd'), tz), formatar_docks(st.get('outbound_dock_infos')), formatar_timestamp_trips(st.get('loading_time'), tz), st.get('unload_quantity', 0), st.get('load_quantity', 0), i.get('vehicle_number'), i.get('driver_name'), i.get('second_driver_name'), "Adhoc" if i.get('trip_source')==1 else "Schedule", i.get('classification_names'), i.get('agency_name'), formatar_timestamp_trips(i.get('mtime'), tz), i.get('operator'), formatar_timestamp_trips(i.get('assigned_time'), tz), i.get('to_inbound_quantity', -1), i.get('order_inbound_quantity', -1), i.get('pack_type', ''), i.get('order_packed_quantity', -1), i.get('to_packed_quantity', -1), i.get('to_loaded_quantity', -1), i.get('order_loaded_quantity', -1), i.get('mtb_loaded_quantity', 0), formatar_timestamp_trips(st.get('add_into_queue_time'), tz), mapear_status_db_all(st.get('trip_station_status'), 'parada'), stats[-1]['station_name'] if stats else '', determinar_turno(st.get('sta'), tz)])
                    proc.add(tid); break
    process(pend); process(dep, True); process(all_tr)
    return combined

def coletar_shopee_historico_ended(driver):
    logging.info("--- [SPX] Ended ---")
    ts = int(time.time())
    res = executar_chamada_api(driver, 'GET', f"{HISTORY_API_URL}?mtime={ts-(86400*3)},{ts+3600}&count=100&pageno=1", "https://spx.shopee.com.br/hubLinehaulTrips/trip")
    dados = []
    if res and res.get("list"):
        for t in res["list"]:
            s = t.get("trip_station", [])
            d = s[-1] if s else {}
            dados.append([t.get("trip_number"), "Ended", t.get("vehicle_number"), t.get("driver_name"), s[0].get("station_name") if s else "", d.get("station_name", ""), formatar_timestamp_unix(d.get("std")), formatar_timestamp_unix(d.get("sta")), formatar_timestamp_unix(d.get("atd")), formatar_timestamp_unix(d.get("ata")), formatar_timestamp_unix(d.get("etd")), formatar_timestamp_unix(d.get("eta")), d.get("load_quantity", 0), d.get("unload_quantity", 0), d.get("expect_unload_quantity", 0), t.get("planning_name"), t.get("id")])
    return dados

def coletar_shopee_queue_log(driver):
    logging.info("--- [SPX] Queue Log (3M) ---")
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    st_date = (now.replace(day=1) - timedelta(days=61)).replace(day=1)
    curr, rows = st_date, []
    while curr < now:
        bend = min(curr + timedelta(days=3), now)
        page = 1
        while True:
            res = executar_chamada_api(driver, 'GET', f"{QUEUE_LOG_API_URL}?update_time={int(curr.timestamp())},{int(bend.timestamp())}&pageno={page}&count=200", "https://spx.shopee.com.br/queue-list/queue-list-log")
            if not res or not res.get("list"): break
            for i in res["list"]:
                if i.get('arrival_type') == 1:
                    rows.append([i.get('queue_id'), i.get('queue_number'), i.get('driver_id'), i.get('driver_name'), i.get('vehicle_number'), i.get('action'), i.get('arrival_type'), "Line Haul", i.get('queue_status'), mapear_status_doca(i.get('queue_status')), formatar_timestamp_unix(i.get('update_time')), i.get('update_time'), i.get('assigned_dock_name'), i.get('occupied_dock_name'), i.get('queue_sequence'), i.get('operator'), i.get('lh_trip_number'), i.get('handover_task_number'), i.get('registration_type')])
            if len(res["list"]) < 200: break
            page += 1
        curr = bend
    return rows

# =================================================================
# FUNÇÕES DE COLETA ILOX
# =================================================================

def coletar_ilox_hora(driver):
    """(Normal) Coleta Dia Atual Hora a Hora - Roda sempre junto com Produtividade."""
    logging.info("--- [ILOX] Hourly DB (Normal) ---")
    tz = pytz.timezone(TIMEZONE)
    agora = datetime.now(tz)
    ini = agora.replace(hour=6,minute=0) if agora.hour >= 6 else (agora-timedelta(days=1)).replace(hour=6,minute=0)
    fim = ini + timedelta(hours=23, minutes=59)
    url_p = f"{ILOX_API_HOURLY_PROD}?unidade_id=1&modo_ativo=PRODUCAO&data_inicio={ini.strftime('%Y-%m-%dT%H:%M')}&data_fim={fim.strftime('%Y-%m-%dT%H:%M')}"
    url_r = f"{ILOX_API_HOURLY_REJ}?unidade_id=1&modo_ativo=PRODUCAO&data_inicio={ini.strftime('%Y-%m-%dT%H:%M')}&data_fim={fim.strftime('%Y-%m-%dT%H:%M')}"
    d_prod = executar_chamada_api(driver, 'GET', url_p, ILOX_DASHBOARD_URL)
    d_rej = executar_chamada_api(driver, 'GET', url_r, ILOX_DASHBOARD_URL)
    if not d_prod or 'labels' not in d_prod: return []
    lbls, prods = d_prod.get('labels', []), d_prod.get('data', [])
    rejs = d_rej.get('data', []) if d_rej else [0]*len(prods)
    rows = []
    ts = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    for i, lbl in enumerate(lbls):
        try:
            val_p = int(prods[i]) if prods[i] is not None else 0
            val_r = int(rejs[i]) if i < len(rejs) and rejs[i] is not None else 0
            rows.append([f"{ini.year}-{lbl.split(' ')[0].split('/')[1]}-{lbl.split(' ')[0].split('/')[0]}", lbl.split(' ')[1], val_p - val_r, val_p, val_r, ts])
        except: continue
    return rows

def coletar_ilox_hora_ontem(driver):
    """(1 Vez ao Dia) Coleta D-1 Hora a Hora."""
    logging.info("--- [ILOX] Hourly DB Ontem (D-1) ---")
    tz = pytz.timezone(TIMEZONE)
    agora = datetime.now(tz)
    dt_ref = agora - timedelta(days=1)
    ini = dt_ref.replace(hour=6, minute=0, second=0, microsecond=0)
    fim = ini + timedelta(hours=23, minutes=59)
    url_p = f"{ILOX_API_HOURLY_PROD}?unidade_id=1&modo_ativo=PRODUCAO&data_inicio={ini.strftime('%Y-%m-%dT%H:%M')}&data_fim={fim.strftime('%Y-%m-%dT%H:%M')}"
    url_r = f"{ILOX_API_HOURLY_REJ}?unidade_id=1&modo_ativo=PRODUCAO&data_inicio={ini.strftime('%Y-%m-%dT%H:%M')}&data_fim={fim.strftime('%Y-%m-%dT%H:%M')}"
    d_prod = executar_chamada_api(driver, 'GET', url_p, ILOX_DASHBOARD_URL)
    d_rej = executar_chamada_api(driver, 'GET', url_r, ILOX_DASHBOARD_URL)
    if not d_prod or 'labels' not in d_prod: return []
    lbls, prods = d_prod.get('labels', []), d_prod.get('data', [])
    rejs = d_rej.get('data', []) if d_rej else [0]*len(prods)
    rows = []
    ts_now = datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    for i, lbl in enumerate(lbls):
        try:
            vp = int(prods[i]) if prods[i] is not None else 0
            vr = int(rejs[i]) if i < len(rejs) and rejs[i] is not None else 0
            rows.append([f"{ini.year}-{lbl.split(' ')[0].split('/')[1]}-{lbl.split(' ')[0].split('/')[0]}", lbl.split(' ')[1], vp-vr, vp, vr, ts_now])
        except: continue
    return rows

def coletar_ilox_historico_prod(driver):
    """(Hora x Hora) Coleta Histórico Detalhado."""
    logging.info("--- [ILOX] Historico Prod (Hora x Hora) ---")
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    # Pega do inicio do dia até a hora atual fechada
    start_date = (now if now.hour >= 6 else now - timedelta(days=1)).replace(hour=6, minute=0, second=0)
    end_date = now + timedelta(hours=1)
    curr, rows = start_date, []
    while curr < end_date:
        next_ptr = curr + timedelta(hours=1)
        s_ini, s_fim = curr.strftime('%Y-%m-%dT%H:%M'), next_ptr.strftime('%Y-%m-%dT%H:%M')
        d = executar_chamada_api(driver, 'GET', f"{ILOX_API_DASHBOARD}?unidade_id=1&modo_ativo=PRODUCAO&periodo=custom&data_inicio={s_ini}&data_fim={s_fim}", ILOX_DASHBOARD_URL)
        if d:
            rows.append([curr.strftime('%Y-%m-%d'), curr.strftime('%H:%M'), d.get('processadosPeriodo',0), d.get('produtosClassificadosCorretamente',0), d.get('rejeitoTotalCalculado',0), d.get('reinducaoTotalCalculado',0), d.get('finalSorterTotalCalculado',0), d.get('rejeitoNoData',0), d.get('rejeitoNoRead',0), d.get('rejeitoCodeNotFind',0), d.get('rejeitoNoDestination',0), d.get('rejeitoNoStandardCode',0), d.get('rejeitoOverLenght',0), d.get('rejeitoDestinationReceivedLate',0), d.get('rejeitoTimeOutWMS',0), d.get('reinducaoNoGap',0), d.get('reinducaoMultiread',0), d.get('finalSorterExitFull',0), d.get('finalSorterNonSortable',0), d.get('finalSorterSideBySide',0), s_ini, s_fim])
        curr = next_ptr
    return rows

# =================================================================
# DRIVERS E MAIN
# =================================================================

def login_shopee(driver):
    driver.get("https://spx.shopee.com.br/")
    try:
        time.sleep(3)
        if "/login" not in driver.current_url: return True
        logging.info("Logando Shopee...")
        # Fecha popup se existir antes de logar
        try: driver.find_element(By.CLASS_NAME, "ant-modal-close-x").click()
        except: pass

        u = WebDriverWait(driver, 30).until(EC.presence_of_element_located((By.XPATH, "//input[@type='text' or contains(@placeholder, 'mail') or contains(@placeholder, 'Ops ID')]")))
        u.click()
        u.send_keys(Keys.CONTROL + "a"); u.send_keys(Keys.DELETE); u.send_keys(SPX_USERNAME)
        
        p = driver.find_element(By.XPATH, "//input[@type='password']")
        p.click()
        p.send_keys(Keys.CONTROL + "a"); p.send_keys(Keys.DELETE); p.send_keys(SPX_PASSWORD)
        p.send_keys(Keys.ENTER)
        
        time.sleep(5)
        # Verifica se logou ou se tem popup bloqueando
        try: driver.find_element(By.TAG_NAME, 'body').send_keys(Keys.ESCAPE)
        except: pass
        
        if "/login" not in driver.current_url:
            logging.info("Login Shopee OK!")
            driver.get("https://spx.shopee.com.br/dashboard/overview")
            return True
        else:
            logging.error("Falha Login Shopee")
    except Exception as e:
        logging.error(f"Erro Login Shopee: {e}")
    return False

def login_ilox(driver):
    driver.get(ILOX_DASHBOARD_URL)
    try:
        time.sleep(5)
        if "login" in driver.current_url.lower() or len(driver.find_elements(By.ID, "usuario")) > 0:
            logging.info("Logando Ilox...")
            driver.find_element(By.ID, "usuario").send_keys(ILOX_EMAIL)
            driver.find_element(By.ID, "senha").send_keys(ILOX_SENHA)
            driver.find_element(By.CSS_SELECTOR, "button.btn-primary").click()
            WebDriverWait(driver, 30).until(EC.url_contains("dashboard"))
            logging.info("Login Ilox OK!")
    except Exception as e:
        logging.error(f"Erro Login Ilox: {e}")
        return False
    return True

def get_driver():
    opts = webdriver.ChromeOptions()
    opts.add_argument("--headless=new")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--log-level=3")
    return webdriver.Chrome(service=ChromeService(ChromeDriverManager().install()), options=opts)

def get_sheets_service():
    if not os.path.exists("service_account.json"):
        creds_env = os.environ.get("GOOGLE_CREDENTIALS_JSON")
        if creds_env:
            with open("service_account.json", "w") as f: f.write(creds_env)
    if os.path.exists("service_account.json"): 
        creds = service_account.Credentials.from_service_account_file("service_account.json", scopes=SCOPES)
    elif os.path.exists("token.json"): 
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    else: 
        logging.error("Nenhuma credencial do Google encontrada.")
        return None
    return build("sheets", "v4", credentials=creds)

def ensure_sheet_exists(service, spreadsheet_id, sheet_name):
    """Verifica se a aba existe. Se não, cria."""
    try:
        sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        sheets = sheet_metadata.get('sheets', [])
        sheet_titles = [s['properties']['title'] for s in sheets]
        if sheet_name not in sheet_titles:
            logging.info(f"Criando aba inexistente: {sheet_name}")
            req = {'requests': [{'addSheet': {'properties': {'title': sheet_name}}}]}
            service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=req).execute()
            time.sleep(1) # Espera propagar
    except Exception as e:
        logging.error(f"Erro ao verificar/criar aba {sheet_name}: {e}")

def write_sheet(service, spreadsheet_id, sheet_name, data, mode="write"):
    if not data: return
    try:
        # --- NOVO: Garante que a aba existe antes de escrever ---
        ensure_sheet_exists(service, spreadsheet_id, sheet_name)
        
        if mode == "write":
            service.spreadsheets().values().clear(spreadsheetId=spreadsheet_id, range=f"'{sheet_name}'").execute()
            service.spreadsheets().values().update(spreadsheetId=spreadsheet_id, range=f"'{sheet_name}'!A1", valueInputOption="USER_ENTERED", body={'values': data}).execute()
        else:
            service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, range=f"'{sheet_name}'!A1", valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS", body={'values': data}).execute()
        
        ts = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        service.spreadsheets().values().append(spreadsheetId=spreadsheet_id, range=f"'{sheet_name}'!A:B", valueInputOption="USER_ENTERED", insertDataOption="INSERT_ROWS", body={'values': [["Última Atualização:", ts]]}).execute()
        logging.info(f"Planilha {sheet_name} atualizada com sucesso.")
    except Exception as e: logging.error(f"Erro Sheet {sheet_name}: {e}")

def main():
    matar_processos_presos()
    sheets = get_sheets_service()
    if not sheets: return

    # Em execuções isoladas via GitHub Actions, essas variáveis locais sempre recomeçarão.
    # Se você precisar de um controle real de tempo entre execuções, o ideal seria salvar o estado em uma célula da própria planilha.
    last_run_queue_log = 0 
    last_run_daily_ilox = None   
    last_run_hourly_prod = None  

    logging.info(">>> CICLO INICIADO (Modo Teste - Planilha Única) <<<")
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    today_str = now.strftime('%Y-%m-%d')
    hour_key = now.strftime('%Y-%m-%d-%H')
    in_time_window = now.minute >= 10 

    driver = None
    try:
        driver = get_driver()
        
        # --- SHOPEE ---
        if login_shopee(driver):
            try:
                d_prod = coletar_shopee_produtividade(driver)
                write_sheet(sheets, MAIN_SPREADSHEET_ID, PRODUTIVIDADE_SHEET_NAME, [["ID", "Nome", "Estação", "Ativ", "Horas", "Thru", "In", "Out", "", "", "H", "D"]] + d_prod)
            except Exception as e: logging.error(f"Erro Prod Real: {e}")

            try:
                d_r, d_f = coletar_shopee_outbound(driver)
                write_sheet(sheets, MAIN_SPREADSHEET_ID, OUTBOUND_ORIGINAL_SHEET_NAME, [["Op", "Total"] + [f"H-{i}" for i in range(12)]] + d_r)
                write_sheet(sheets, MAIN_SPREADSHEET_ID, OUTBOUND_SHEET_NAME, [["Op", "Total", "Hora", "Eficiência"]] + d_f)
                
                d_dock = coletar_shopee_dock(driver)
                write_sheet(sheets, MAIN_SPREADSHEET_ID, DOCK_QUEUE_SHEET_NAME, [["Q", "Veh", "Wait", "Pri", "Tags", "Hld", "Trip", "TName", "Task", "Qty", "Driv", "Typ", "Ag", "Prt", "Asg", "Grp", "Occ", "Sts", "Seq", "Act"]] + d_dock)
                
                d_all = coletar_shopee_db_all(driver)
                write_sheet(sheets, MAIN_SPREADSHEET_ID, ALL_TRIPS_SHEET_NAME, [["Trip","Ont","Veh","STA","STD","ATA","ATD","ETA","ETD","Dock","LoadT","Unl","Ld","Plt","Dr","Dr2","Src","Cls","Ag","Upd","Op","Asg","Inb","OInb","Pck","OPck","TPck","TLd","OLd","MTB","AddQ","Sts","Dest","Trn"]] + d_all)
                
                d_end = coletar_shopee_historico_ended(driver)
                write_sheet(sheets, MAIN_SPREADSHEET_ID, HISTORY_SHEET_NAME, [["Trp","Sts","Plt","Dr","Ori","Dst","STD","STA","ATD","ATA","ETD","ETA","Ld","Unl","Exp","Pln","ID"]] + d_end)
            except Exception as e: logging.error(f"Erro Geral Shopee: {e}")

            # Como o código rodará isolado a cada 5min, last_run_queue_log sempre será 0.
            # Isso significa que ele vai puxar a Queue Log em toda execução. Se for muito pesado, me avise para ajustarmos!
            if time.time() - last_run_queue_log > 7200:
                try:
                    d_q = coletar_shopee_queue_log(driver)
                    write_sheet(sheets, MAIN_SPREADSHEET_ID, QUEUE_LOG_SHEET_NAME, [["ID","QNo","DID","DName","Plate","Act","Arr","Desc","StID","StDesc","Upd","Ts","Asg","Occ","Seq","Op","Trp","Tsk","Reg"]] + d_q)
                    last_run_queue_log = time.time()
                except: pass
        
        if driver:
            driver.quit()
            driver = None

    except Exception as e: 
        logging.error(f"Erro Fatal Ciclo Shopee: {e}")
        if driver: driver.quit()

    # --- ILOX ---
    driver = None
    try:
        driver = get_driver()
        if login_ilox(driver):
            try:
                d_hr = coletar_ilox_hora(driver)
                if d_hr: write_sheet(sheets, MAIN_SPREADSHEET_ID, ILOX_HOURLY_SHEET_NAME, [["Data", "Hora", "Pacotes - Rejeitos ", "Pacotes", "Rejeitos", "Upd"]] + d_hr)
            except Exception as e: logging.error(f"Erro Ilox Hourly DB: {e}")

            if now.hour == 7 and in_time_window and last_run_daily_ilox != today_str:
                try:
                    d_ontem = coletar_ilox_hora_ontem(driver)
                    if d_ontem:
                        write_sheet(sheets, MAIN_SPREADSHEET_ID, HOURLY_DB_ONTEM_SHEET_NAME, [["Dt", "Hr", "Pct-Rej", "Pct", "Rej", "Upd"]] + d_ontem)
                        last_run_daily_ilox = today_str 
                except Exception as e: logging.error(f"Erro Ilox D-1: {e}")

            if in_time_window and last_run_hourly_prod != hour_key:
                try:
                    d_hist = coletar_ilox_historico_prod(driver)
                    if d_hist:
                        h = ["Dt", "Hr", "Proc", "Cls", "RejT", "ReindT", "SortT", "NoDt", "NoRd", "NoCd", "NoDst", "NoStd", "Over", "Late", "Tout", "NoGp", "Mul", "Full", "NSort", "Side", "Ini", "Fim"]
                        write_sheet(sheets, MAIN_SPREADSHEET_ID, HISTORICO_PROD_SHEET_NAME, [h] + d_hist)
                        last_run_hourly_prod = hour_key
                except Exception as e: logging.error(f"Erro Ilox Hist Prod: {e}")

        if driver: driver.quit()
    except Exception as e:
        logging.error(f"Erro Fatal Ciclo Ilox: {e}")
        if driver: driver.quit()

    gc.collect()
    logging.info(">>> CICLO FINALIZADO <<<")

if __name__ == "__main__":
    main()
