"""
GeoSales Analytics — app.py
Todo en un solo archivo: cero imports locales, funciona en Streamlit Cloud sin configuración extra.
"""
import io, re, time, requests
import numpy as np
import pandas as pd
import streamlit as st
from datetime import timedelta
from dataclasses import dataclass

st.set_page_config(page_title="GeoSales Analytics", page_icon="🗺️",
                   layout="wide", initial_sidebar_state="expanded")

# ══════════════════════════════════════════════════════════════════════════════
# FOLIUM (importación condicional)
# ══════════════════════════════════════════════════════════════════════════════
try:
    import folium
    from folium.plugins import HeatMap, MarkerCluster
    from streamlit_folium import st_folium
    _FOLIUM = True
except ImportError:
    _FOLIUM = False

# ══════════════════════════════════════════════════════════════════════════════
# SESIÓN
# ══════════════════════════════════════════════════════════════════════════════
def init_session():
    D = {
        "df_original":None,"df_working":None,"df_filtered":None,
        "df_normalized":None,"df_geocoded":None,"df_clustered":None,
        "step_uploaded":False,"step_mapped":False,"step_filtered":False,
        "step_normalized":False,"step_geocoded":False,"step_clustered":False,
        "col_mapping":{},"google_api_key":"","geo_cache":{},
        "cluster_method":"KMeans","cluster_weight":"kg",
        "n_clusters":8,"file_name":"","stop_geocoding":False,"codo_df":None,
    }
    for k,v in D.items():
        if k not in st.session_state: st.session_state[k] = v

def get_active_df():
    for k in ("df_clustered","df_geocoded","df_normalized","df_filtered","df_working","df_original"):
        df = st.session_state.get(k)
        if df is not None and not df.empty: return df.copy()
    return None

def pipeline_df():
    for k in ("df_geocoded","df_normalized","df_filtered","df_working","df_original"):
        df = st.session_state.get(k)
        if df is not None and not df.empty: return df
    return None

def invalidate_from(step):
    order=[("normalize","df_normalized","step_normalized"),
           ("geocode","df_geocoded","step_geocoded"),
           ("cluster","df_clustered","step_clustered")]
    go=False
    for s,dk,fk in order:
        if s==step: go=True
        if go: st.session_state[dk]=None; st.session_state[fk]=False

# ══════════════════════════════════════════════════════════════════════════════
# HELPERS DE COLUMNAS
# ══════════════════════════════════════════════════════════════════════════════
def lat_col(df): return next((c for c in df.columns if c.lower() in ("latitud","lat","latitude")),None)
def lon_col(df): return next((c for c in df.columns if c.lower() in ("longitud","lon","lng","longitude")),None)
def kg_col(df):  return next((c for c in df.columns if c.lower() in ("kg","kilogramos","peso")),None)
def ped_col(df): return next((c for c in df.columns if c.lower() in ("num_pedidos","pedidos","orders")),None)

# ══════════════════════════════════════════════════════════════════════════════
# NORMALIZACIÓN — GOOGLE MAPS COLOMBIA — COBERTURA NACIONAL COMPLETA
# Soporta 1000+ patrones reales: urbanos, rurales, industriales, veredas
# Entrada:  "CL 15 7 25 BRR EL RAICERO,FLORENCIA - CAQUETÁ"
# Salida:   "Calle 15 #7-25 Barrio EL RAICERO, Florencia, Caquetá, Colombia"
# ══════════════════════════════════════════════════════════════════════════════

# Tipos de vía — orden crítico: más específicos primero
VIA_TYPES = [
    # Avenidas compuestas (antes que las simples)
    (r'\bAV(?:DA?)?\.?\s+(?:CL|CLLE?|CALLE)\b',         "Avenida Calle"),
    (r'\bAV(?:DA?)?\.?\s+(?:CRA?|KR|CARRERA)\b',        "Avenida Carrera"),
    (r'\bAC\b\.?',   "Avenida Calle"),    # AC = Avenida Calle (Bogotá)
    (r'\bAK\b\.?',   "Avenida Carrera"),  # AK = Avenida Carrera (Bogotá)
    (r'\b(?:AV|AVDA?|AVENIDA)\b\.?', "Avenida"),
    # Tipos principales
    (r'\b(?:CARRERA|CRA|KRA|CR|KR|AKR)\b\.?',   "Carrera"),
    (r'\b(?:CALLE|CLLE?|CLL|CL|Cl)\b\.?',       "Calle"),
    (r'\b(?:DIAGONAL|DIAG?|DG)\b\.?',            "Diagonal"),
    (r'\b(?:TRANSVERSAL|TRANSV?|TRV|TV)\b\.?',   "Transversal"),
    # Autopista — AUT es MUY frecuente en tu BD (AUT MED BOG, AUT NORTE)
    (r'\b(?:AUTOPISTA|AUT(?:OP)?)\b\.?',         "Autopista"),
    (r'\b(?:CIRCUNVALAR|CIRCUNV?|CIRC)\b\.?',    "Circunvalar"),
    (r'\bVARIANTE\b\.?',                          "Variante"),
    (r'\bBULEVAR\b\.?',                           "Bulevar"),  # BULEVAR aparece en BD
    # Vías rurales y especiales
    (r'\b(?:KILOMETRO|KM)\b\.?',                 "Km"),
]

# Complementos internos de la dirección
COMPLEMENTOS = [
    # Barrio — BRR muy frecuente, también BRR. con punto
    (r'\bBRR\.?\b',              "Barrio"),
    (r'\bBR\b\.?(?=\s+[A-ZÁÉÍÓÚÑ])', "Barrio"),
    # Rural
    (r'\bVDA\b\.?',              "Vereda"),   # VDA = Vereda
    (r'\bVRD\b\.?',              "Vereda"),   # VRD alternativo
    (r'\bFCA\b\.?',              "Finca"),    # FCA = Finca
    (r'\bSEC\b\.?(?=\s)',        "Sector"),   # SEC = Sector
    (r'\bSECTOR\b\.?',          "Sector"),
    # Industrial/Comercial
    (r'\bZN\b\.?(?=\s+(?:IND|FRANCA|INDUSTRIAL|LOGIS))', "Zona"),
    (r'\bZN\b\.?(?=\s)',         "Zona"),     # ZN genérico
    (r'\bZONA\b\.?',             "Zona"),
    (r'\bZF\b\.?',               "Zona Franca"),  # ZF = Zona Franca
    (r'\bTZ\b\.?',               "Terraza"),  # TZ = Terraza (parques industriales)
    (r'\bPAR\b\.?(?=\s+(?:IND|INDUSTRIAL|AGR|AGROIN))', "Parque"),
    (r'\bPAR\b\.?(?=\s)',        "Parque"),
    (r'\bPQ\b\.?(?=\s)',         "Parque"),   # PQ = Parque industrial
    (r'\bCIUDADELA\b',           "Ciudadela"),
    (r'\bANILLO\s+VIAL\b',       "Anillo Vial"),
    (r'\bVTE\b\.?',              "Variante"),  # VTE = Variante
    (r'\bVT\b\.?(?=\s+[A-Z])',   "Variante"),
    # Unidades internas
    (r'\bBG\b\.?',               "Bodega"),
    (r'\bBOD\b\.?',              "Bodega"),
    (r'\bLC\b\.?',               "Local"),
    (r'\bLOCAL\b\.?',            "Local"),
    (r'\bCA\b\.?(?=\s+\d)',      "Casa"),
    (r'\bCS\b\.?',               "Casa"),
    (r'\bAP(?:TO)?\b\.?',        "Apartamento"),
    (r'\bINT\b\.?',              "Interior"),
    (r'\bOF\b\.?',               "Oficina"),
    (r'\bOFICINA\b\.?',          "Oficina"),
    (r'\bTO(?:RRE)?\b\.?(?=\s+\d)', "Torre"),
    (r'\bED(?:IF)?\b\.?',        "Edificio"),
    (r'\bMZ\b\.?',               "Manzana"),
    (r'\bLT\b\.?',               "Lote"),
    (r'\bET\b\.?',               "Etapa"),
    (r'\bURB?\b\.?(?=\s+[A-Z])', "Urbanización"),
    (r'\bBLQ\b\.?',              "Bloque"),
    (r'\bPIS\b\.?',              "Piso"),
    (r'\bP\b\.?(?=\s+\d)',       "Piso"),     # P 1 = Piso 1
    (r'\bAL\b\.?(?=\s+\d)',      "Apartamento Local"),
]

DPTOS_MAP = {
    "ANTIOQUIA":"Antioquia","CUNDINAMARCA":"Cundinamarca",
    "VALLE DEL CAUCA":"Valle del Cauca","VALLE":"Valle del Cauca",
    "ATLANTICO":"Atlántico","ATLÁNTICO":"Atlántico",
    "BOLIVAR":"Bolívar","BOLÍVAR":"Bolívar",
    "SANTANDER":"Santander","NORTE DE SANTANDER":"Norte de Santander",
    "BOYACA":"Boyacá","BOYACÁ":"Boyacá",
    "CALDAS":"Caldas","RISARALDA":"Risaralda",
    "QUINDIO":"Quindío","QUINDÍO":"Quindío",
    "TOLIMA":"Tolima","HUILA":"Huila","CAUCA":"Cauca",
    "NARIÑO":"Nariño","NARINO":"Nariño","META":"Meta",
    "CASANARE":"Casanare","ARAUCA":"Arauca","VICHADA":"Vichada",
    "AMAZONAS":"Amazonas","PUTUMAYO":"Putumayo",
    "CAQUETA":"Caquetá","CAQUETÁ":"Caquetá","GUAVIARE":"Guaviare",
    "CHOCO":"Chocó","CHOCÓ":"Chocó","CESAR":"Cesar",
    "LA GUAJIRA":"La Guajira","MAGDALENA":"Magdalena",
    "SUCRE":"Sucre","CORDOBA":"Córdoba","CÓRDOBA":"Córdoba",
    "ARCHIPIELAGO DE SAN ANDRES":"San Andrés",
}

CIUDADES_MAP = {
    r'\bBOGOT[AÁ]\b':"Bogotá", r'\bMEDELL[IÍ]N\b':"Medellín",
    r'\bC[UÚ]CUT[AÁ]\b':"Cúcuta", r'\bIBAGU[EÉ]\b':"Ibagué",
    r'\bPOPAY[AÁ]N\b':"Popayán", r'\bBUC?ARAMANGA\b':"Bucaramanga",
    r'\bMONTER[IÍ]A\b':"Montería", r'\bVALLEDUPAR\b':"Valledupar",
    r'\bBAR(?:R)?ANQUILLA\b':"Barranquilla",
    r'\bSANTA\s+MARTA\b':"Santa Marta",
    r'\bVILLAVICENCIO\b':"Villavicencio", r'\bCARTAGENA\b':"Cartagena",
    r'\bPASTO\b':"Pasto", r'\bCALI\b':"Cali",
    r'\bPEREIRA\b':"Pereira", r'\bMANIZALES\b':"Manizales",
    r'\bARMENIA\b':"Armenia", r'\bTUNJA\b':"Tunja",
    r'\bNEIVA\b':"Neiva", r'\bFLORENCIA\b':"Florencia",
    r'\bSOGAMOSO\b':"Sogamoso", r'\bDOSQUEBRADAS\b':"Dosquebradas",
    r'\bPITALITO\b':"Pitalito", r'\bGUARNE\b':"Guarne",
    r'\bCALDAS\b':"Caldas", r'\bSINCELEJO\b':"Sincelejo",
    r'\bRIOHACHA\b':"Riohacha", r'\bMONTERIA\b':"Montería",
    r'\bTURBO\b':"Turbo", r'\bAPARTADO\b':"Apartadó",
    r'\bITAGUI\b':"Itagüí", r'\bENVIGADO\b':"Envigado",
    r'\bBELLO\b':"Bello", r'\bSABANETA\b':"Sabaneta",
    r'\bLA\s+ESTRELLA\b':"La Estrella", r'\bCOPACABANA\b':"Copacabana",
    r'\bGIRON\b':"Girón", r'\bGIRÓN\b':"Girón",
    r'\bFLORIDABLANCA\b':"Floridablanca", r'\bSOLEDAD\b':"Soledad",
    r'\bMALAMBO\b':"Malambo", r'\bRIONEGRO\b':"Rionegro",
    r'\bLA\s+CEJA\b':"La Ceja", r'\bMAR[IÍ]NILLA\b':"Marinilla",
    r'\bZIPAQUIR[AÁ]\b':"Zipaquirá", r'\bCHÍA\b':"Chía",
    r'\bCHIA\b':"Chía", r'\bFUNZA\b':"Funza",
    r'\bMOSQUERA\b':"Mosquera", r'\bCOTA\b':"Cota",
    r'\bTOCANIP[AÁ]\b':"Tocancipá", r'\bFACATATIV[AÁ]\b':"Facatativá",
    r'\bMADRID\b(?=\s*[,\-])':"Madrid", r'\bSIBAT[EÉ]\b':"Sibaté",
    r'\bSOPO\b':"Sopó", r'\bSOP[OÓ]\b':"Sopó",
    r'\bCOGUA\b':"Cogua", r'\bLEBRIJA\b':"Lebrija",
    r'\bFUSAGASUG[AÁ]\b':"Fusagasugá", r'\bGIRAR?DOT\b':"Girardot",
    r'\bYOPAL\b':"Yopal", r'\bVILLAVICENCIO\b':"Villavicencio",
    r'\bQUIMBAYA\b':"Quimbaya", r'\bCALARC[AÁ]\b':"Calarcá",
    r'\bSAN\s+GIL\b':"San Gil", r'\bBUCARAMANGA\b':"Bucaramanga",
    r'\bDUITAMA\b':"Duitama", r'\bCHIQUINQUIR[AÁ]\b':"Chiquinquirá",
    r'\bGRANADA\b(?=\s*[,\-])':"Granada", r'\bCAMPO\s+ALEGRE\b':"Campo Alegre",
    r'\bSAN\s+VICENTE\b':"San Vicente", r'\bPUERTO\s+GAIT[AÁ]N\b':"Puerto Gaitán",
    r'\bEL\s+CARMEN\s+DE\s+VIBORAL\b':"El Carmen de Viboral",
    r'\bCARMEN\s+DE\s+VIBORAL\b':"El Carmen de Viboral",
    r'\bPUERTO\s+LOPEZ\b':"Puerto López",
    r'\bVILLANUEVA\b':"Villanueva", r'\bGRANADA\b':"Granada",
    r'\bISNOS\b':"Isnos", r'\bLA\s+PLATA\b':"La Plata",
    r'\bSAN\s+VICENTE\b':"San Vicente",
}


def _convert_num(m):
    via=m.group(1).strip(); n1=m.group(2); l1=m.group(3) or ""
    n2=m.group(4); ori=(m.group(5) or "").strip()
    return f"{via} #{n1}{l1}-{n2}" + (f" {ori}" if ori else "")


def norm_addr(addr: str, city_suffix: str = "Colombia") -> str:
    """
    Normaliza direcciones colombianas (urbanas, rurales, industriales)
    al formato óptimo para Google Maps Geocoding API.
    """
    if not isinstance(addr, str) or not addr.strip():
        return ""

    # 1. Separar vía y ciudad por la primera coma
    partes     = addr.strip().split(',', 1)
    via_raw    = partes[0].strip()
    ciudad_raw = partes[1].strip() if len(partes) > 1 else ""

    # 2. Parsear ciudad y departamento
    m_paren = re.match(r'^(.+?)\s*\(([^)]+)\)\s*$', ciudad_raw)
    if m_paren:
        ciudad_raw = m_paren.group(1).strip()
        dpto_raw   = m_paren.group(2).strip()
    elif ' - ' in ciudad_raw:
        parts = ciudad_raw.split(' - ', 1)
        ciudad_raw = parts[0].strip()
        dpto_raw   = parts[1].strip()
    else:
        dpto_raw = ""

    # 3. Normalizar ciudad (sobre texto original para capturar tildes)
    ciudad_norm = ciudad_raw
    for pat, rep in CIUDADES_MAP.items():
        ciudad_norm = re.sub(pat, rep, ciudad_norm, flags=re.IGNORECASE)
    if ciudad_norm == ciudad_norm.upper():
        ciudad_norm = ciudad_norm.title()

    # 4. Normalizar departamento
    dpto_norm = DPTOS_MAP.get(dpto_raw.upper().strip(), "")
    if not dpto_norm and dpto_raw:
        dpto_norm = dpto_raw.title()

    # 5. Normalizar la parte de la vía
    s = via_raw.upper().strip()
    s = re.sub(r'\s+', ' ', s)

    # Expandir tipos de vía
    for pat, rep in VIA_TYPES:
        s = re.sub(pat, rep, s, flags=re.IGNORECASE)

    # Expandir complementos (BRR, VDA, BG, TZ, ZN, etc.)
    for pat, rep in COMPLEMENTOS:
        s = re.sub(pat, rep, s, flags=re.IGNORECASE)

    # Convertir numeración "N1 N2" → "#N1-N2" (solo para vías con número)
    if '#' not in s and not re.search(r'\d-\d', s):
        s = re.sub(
            r'((?:Calle|Carrera|Diagonal|Transversal|Avenida(?:\s+\w+)?|'
            r'Autopista|Circunvalar|Variante|Bulevar|Km)'
            r'(?:\s+\d+[A-Za-z]?)(?:\s+(?:Bis|Sur|Norte|Este|Oeste|Sur))*)'
            r'\s+(\d+)([A-Za-z]?)\s+(\d+)'
            r'(\s+(?:Sur|Norte|Este|Oeste))?',
            _convert_num, s, flags=re.IGNORECASE
        )

    # Capitalización de orientaciones
    for pat, rep in [(r'\bSUR\b','Sur'),(r'\bNORTE\b','Norte'),
                     (r'\bESTE\b','Este'),(r'\bOESTE\b','Oeste'),(r'\bBIS\b','Bis')]:
        s = re.sub(pat, rep, s, flags=re.IGNORECASE)

    s = re.sub(r'\s+', ' ', s).strip()

    # 6. Construir resultado final — Ciudad y Dpto mejoran precisión
    partes_final = [p for p in [
        s,
        ciudad_norm if ciudad_norm else None,
        dpto_norm   if dpto_norm and dpto_norm.lower() != ciudad_norm.lower() else None,
        "Colombia",
    ] if p]
    return ", ".join(partes_final)


def norm_col(df, addr_col, country="Colombia", out="dir_normalizada"):
    df = df.copy()
    df[out] = df[addr_col].astype(str).apply(lambda a: norm_addr(a, country))
    return df


def addr_stats(df, col):
    t = len(df)
    e = int(df[col].isna().sum() + (df[col].astype(str).str.strip() == "").sum())
    n = int(df[col].astype(str).str.contains(r'\d', na=False).sum())
    return {"total": t, "empty": e, "with_num": n,
            "pct_ok": round(n / max(t - e, 1) * 100, 1)}


# ══════════════════════════════════════════════════════════════════════════════
# GEOCODIFICACIÓN
# ══════════════════════════════════════════════════════════════════════════════
GURL = "https://maps.googleapis.com/maps/api/geocode/json"

def geo_one(addr, key):
    try:
        d = requests.get(GURL,params={"address":addr,"key":key},timeout=10).json()
        s = d.get("status","ERROR")
        if s=="OK":
            loc=d["results"][0]["geometry"]["location"]
            return loc["lat"],loc["lng"],"ok"
        if s=="ZERO_RESULTS": return None,None,"zero"
        if s in ("OVER_DAILY_LIMIT","OVER_QUERY_LIMIT","REQUEST_DENIED"): return None,None,"quota"
        return None,None,f"api:{s}"
    except: return None,None,"error"

def validate_key(key):
    if not key or len(key)<20: return False,"❌ Clave demasiado corta"
    _,_,s = geo_one("Bogotá, Colombia",key)
    if s=="ok":    return True, "✅ Clave válida — Google Maps responde correctamente"
    if s=="quota": return False,"❌ Clave denegada o cuota agotada"
    return False, f"❌ Error: {s}"

def geo_df(df, addr_col, key, delay=0.05, cb=None):
    df=df.copy()
    cache=st.session_state.setdefault("geo_cache",{})
    uniq=df[addr_col].dropna().astype(str).unique().tolist()
    pend=[a for a in uniq if a not in cache]
    stats={"unique":len(uniq),"from_cache":len(uniq)-len(pend),"ok":0,"zero":0,"errors":0,"quota":False}
    for i,addr in enumerate(pend):
        if st.session_state.get("stop_geocoding",False): break
        lt,ln,s=geo_one(addr,key); cache[addr]=(lt,ln)
        if s=="ok": stats["ok"]+=1
        elif s=="zero": stats["zero"]+=1
        elif s=="quota": stats["quota"]=True; stats["errors"]+=1; break
        else: stats["errors"]+=1
        if cb: cb(i+1,len(pend))
        time.sleep(delay)
    st.session_state["geo_cache"]=cache
    df["latitud"]  = df[addr_col].astype(str).map(lambda a: cache.get(a,(None,None))[0])
    df["longitud"] = df[addr_col].astype(str).map(lambda a: cache.get(a,(None,None))[1])
    stats["geocoded"]=int(df["latitud"].notna().sum())
    stats["pct"]=round(stats["geocoded"]/max(len(df),1)*100,1)
    return df,stats

def check_coords(df, lc, oc):
    if lc not in df.columns or oc not in df.columns:
        return {"has":False,"valid":0,"total":len(df),"pct":0}
    lt=pd.to_numeric(df[lc],errors="coerce"); ln=pd.to_numeric(df[oc],errors="coerce")
    v=(lt.between(-90,90)&ln.between(-180,180)&lt.notna()&ln.notna()).sum()
    return {"has":int(v)>0,"valid":int(v),"total":len(df),"pct":round(int(v)/max(len(df),1)*100,1)}

# ══════════════════════════════════════════════════════════════════════════════
# GEOCODIFICACIÓN ASÍNCRONA
# ══════════════════════════════════════════════════════════════════════════════
def geo_df_async(df, addr_col, key, max_workers=5, delay=0.05, cb=None):
    """
    Geocodificación paralela usando ThreadPoolExecutor.
    - Divide las direcciones únicas pendientes entre N workers.
    - Cada worker respeta un delay mínimo para no saturar la API.
    - Mucho más rápida que la versión secuencial para >500 dirs.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed
    import threading

    df = df.copy()
    cache = st.session_state.setdefault("geo_cache", {})
    uniq  = df[addr_col].dropna().astype(str).unique().tolist()
    pend  = [a for a in uniq if a not in cache]

    stats = {"unique": len(uniq), "from_cache": len(uniq)-len(pend),
             "ok": 0, "zero": 0, "errors": 0, "quota": False}

    lock        = threading.Lock()
    done_count  = [0]
    stop_flag   = [False]

    def _worker(addr):
        if stop_flag[0]:
            return addr, None, None, "stopped"
        time.sleep(delay)
        lt, ln, s = geo_one(addr, key)
        return addr, lt, ln, s

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(_worker, a): a for a in pend}
        for fut in as_completed(futures):
            if st.session_state.get("stop_geocoding", False):
                stop_flag[0] = True
            addr, lt, ln, s = fut.result()
            with lock:
                cache[addr] = (lt, ln)
                if s == "ok":    stats["ok"]    += 1
                elif s == "zero":stats["zero"]  += 1
                elif s == "quota":
                    stats["quota"] = True
                    stats["errors"] += 1
                    stop_flag[0] = True
                elif s != "stopped":
                    stats["errors"] += 1
                done_count[0] += 1
                if cb: cb(done_count[0], len(pend))

    st.session_state["geo_cache"] = cache
    df["latitud"]  = df[addr_col].astype(str).map(lambda a: cache.get(a,(None,None))[0])
    df["longitud"] = df[addr_col].astype(str).map(lambda a: cache.get(a,(None,None))[1])
    stats["geocoded"]     = int(df["latitud"].notna().sum())
    stats["pct"]          = round(stats["geocoded"]/max(len(df),1)*100, 1)
    return df, stats


# ══════════════════════════════════════════════════════════════════════════════
# CLUSTERIZACIÓN
# ══════════════════════════════════════════════════════════════════════════════
@dataclass
class Meth:
    label:str; desc:str; pros:list; cons:list; hint:str

METHODS = {
    "KMeans": Meth("K-Means","Divide en K zonas minimizando distancia al centroide.",
        ["Simple","Rápido","K fijo = zonas predefinidas"],
        ["Requiere K","Sensible a outliers","Tamaños similares"],
        "**n_clusters**: número de zonas."),
    "MiniBatchKMeans": Meth("Mini-Batch K-Means","KMeans más rápido para >50k puntos.",
        ["Muy rápido en grandes volúmenes","Mismo concepto KMeans"],
        ["Ligera pérdida precisión","Requiere K"],
        "**n_clusters**: zonas. **batch_size**: lote."),
    "DBSCAN": Meth("DBSCAN","Agrupa por densidad. No requiere K. Detecta outliers.",
        ["Sin K","Detecta outliers","Cualquier forma de cluster"],
        ["Sensible a eps/min_samples","Difícil de ajustar"],
        "**eps** (~0.009°≈1km). **min_samples**: mínimo puntos."),
}
WEIGHTS = {
    "sin_peso":   "Sin peso — todos iguales",
    "kg":         "⚖️ KG — volumen de entrega",
    "pedidos":    "📦 Pedidos — frecuencia de compra",
    "cantidad":   "🔢 Cantidad — unidades vendidas",
    "kg_ped":     "⚡ KG + Pedidos (60% + 40%)",
    "kg_cant":    "⚡ KG + Cantidad (60% + 40%)",
    "ped_cant":   "⚡ Pedidos + Cantidad (60% + 40%)",
    "todo":       "🌟 KG + Pedidos + Cantidad (combinado)",
}

def build_w(df, wt, kgc, pedc, cantc=None):
    """
    Construye el array de pesos para la clusterización ponderada.
    kgc:   columna de KG
    pedc:  columna de Pedidos
    cantc: columna de Cantidad
    Modos: sin_peso | kg | pedidos | cantidad | kg_ped | kg_cant | ped_cant | todo
    """
    if wt == "sin_peso": return None
    def safe(c):
        return pd.to_numeric(df[c], errors="coerce").fillna(0).clip(lower=0).values                if c and c in df.columns else np.zeros(len(df))
    def norm(a): mx=a.max(); return a/mx if mx>0 else a

    if   wt == "kg":       w = safe(kgc)
    elif wt == "pedidos":  w = safe(pedc)
    elif wt == "cantidad": w = safe(cantc)
    elif wt == "kg_ped":   w = norm(safe(kgc))*0.6 + norm(safe(pedc))*0.4
    elif wt == "kg_cant":  w = norm(safe(kgc))*0.6 + norm(safe(cantc))*0.4
    elif wt == "ped_cant": w = norm(safe(pedc))*0.6 + norm(safe(cantc))*0.4
    elif wt == "todo":
        # Combinar las tres variables con pesos iguales tras normalizar
        w = (norm(safe(kgc)) + norm(safe(pedc)) + norm(safe(cantc))) / 3.0
    else:
        return None
    return np.clip(w, 0, None)

def do_cluster(df, lc, oc, method, params, wt, kgc, pedc, cantc=None):
    df=df.copy()
    ln=pd.to_numeric(df[lc],errors="coerce"); lo=pd.to_numeric(df[oc],errors="coerce")
    v=ln.notna()&lo.notna()&ln.between(-90,90)&lo.between(-180,180)
    dv,dx=df[v].copy(),df[~v].copy()
    if len(dv)<3: return df,{"error":f"Solo {len(dv)} puntos válidos."}
    X=np.column_stack([ln[v].values,lo[v].values]); W=build_w(dv,wt,kgc,pedc,cantc)
    if method=="KMeans":
        from sklearn.cluster import KMeans
        k=min(params.get("n_clusters",8),len(dv))
        labels=KMeans(n_clusters=k,random_state=42,n_init="auto").fit(X,sample_weight=W).labels_
    elif method=="MiniBatchKMeans":
        from sklearn.cluster import MiniBatchKMeans
        k=min(params.get("n_clusters",8),len(dv))
        labels=MiniBatchKMeans(n_clusters=k,batch_size=params.get("batch_size",1024),
                               random_state=42,n_init="auto").fit(X,sample_weight=W).labels_
    elif method=="DBSCAN":
        from sklearn.cluster import DBSCAN
        labels=DBSCAN(eps=params.get("eps",0.01),min_samples=params.get("min_samples",5),
                      metric="haversine",algorithm="ball_tree").fit_predict(np.radians(X))
    else: return df,{"error":f"Método desconocido: {method}"}
    dv["cluster"]=labels; dx["cluster"]=-99
    dr=pd.concat([dv,dx]).sort_index()
    vl=labels[labels>=0]; noise=int((labels==-1).sum())
    agg={}
    if kgc  and kgc  in dv.columns: agg[kgc] ="sum"
    if pedc and pedc in dv.columns: agg[pedc]="sum"
    cnt=dv.assign(cluster=labels).groupby("cluster").size().rename("n_puntos")
    smr=(dv.assign(cluster=labels).groupby("cluster").agg(agg).join(cnt).reset_index()
         if agg else cnt.reset_index())
    return dr,{"n_clusters":len(set(vl)),"n_points":len(dv),"noise":noise,
               "pct_noise":round(noise/max(len(dv),1)*100,1),"summary":smr}

def comparar_k(df, lc, oc, k_values, weight_type="sin_peso", kgc=None, pedc=None, cantc=None):
    """
    Corre KMeans para múltiples K y retorna Inercia + Silhouette Score.
    Usar para elegir K óptimo con el método del codo.
    """
    from sklearn.cluster import KMeans
    from sklearn.metrics import silhouette_score
    lat_n=pd.to_numeric(df[lc],errors="coerce"); lon_n=pd.to_numeric(df[oc],errors="coerce")
    valid=lat_n.notna()&lon_n.notna()&lat_n.between(-90,90)&lon_n.between(-180,180)
    X=np.column_stack([lat_n[valid].values,lon_n[valid].values])
    W=build_w(df[valid],weight_type,kgc,pedc,cantc)
    rows=[]
    for k in k_values:
        if k>=len(X): continue
        km=KMeans(n_clusters=k,random_state=42,n_init="auto")
        labels=km.fit_predict(X,sample_weight=W)
        sil=float(silhouette_score(X,labels)) if 2<=k<len(X) else None
        rows.append({"K":k,"Inercia":round(float(km.inertia_),1),
                     "Silhouette":round(sil,4) if sil is not None else None,
                     "Calidad":("Excelente" if sil and sil>0.5 else
                                "Bueno"     if sil and sil>0.25 else
                                "Regular"   if sil and sil>0.0  else "Pobre") if sil else "-"})
    return pd.DataFrame(rows)


# ══════════════════════════════════════════════════════════════════════════════
# MAPAS
# ══════════════════════════════════════════════════════════════════════════════
MCOL=["#e63946","#2196f3","#4caf50","#ff9800","#9c27b0","#00bcd4","#f44336",
      "#8bc34a","#ffeb3b","#795548","#607d8b","#e91e63","#03a9f4","#cddc39","#ff5722"]
TILES={"Oscuro (CartoDB)":"CartoDB dark_matter","Claro (CartoDB)":"CartoDB positron",
       "OpenStreetMap":"OpenStreetMap",
       "Satélite (ESRI)":("https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}","ESRI")}

def mcol(cid): return "#888" if cid<0 else MCOL[cid%len(MCOL)]
def ctr(df,lc,oc):
    lt=pd.to_numeric(df[lc],errors="coerce").dropna(); lo=pd.to_numeric(df[oc],errors="coerce").dropna()
    return (float(lt.mean()) if not lt.empty else 4.711, float(lo.mean()) if not lo.empty else -74.072)
def bmap(c,tile,zoom=11):
    t=TILES.get(tile,"CartoDB dark_matter")
    return folium.Map(location=c,zoom_start=zoom,tiles=t[0],attr=t[1]) if isinstance(t,tuple) \
           else folium.Map(location=c,zoom_start=zoom,tiles=t)
def vdf(df,lc,oc):
    return df[pd.to_numeric(df[lc],errors="coerce").notna()&
              pd.to_numeric(df[oc],errors="coerce").notna()].copy()

def mapa_puntos(df,lc,oc,tips,tile,color):
    dv=vdf(df,lc,oc); m=bmap(ctr(dv,lc,oc),tile)
    for _,r in dv.iterrows():
        p=[f"<b>{c}</b>: {r.get(c,'')}" for c in tips if c in r.index]
        folium.CircleMarker([float(r[lc]),float(r[oc])],radius=5,color=color,
            fill=True,fill_color=color,fill_opacity=0.75,
            tooltip=folium.Tooltip("<br>".join(p)) if p else None).add_to(m)
    return m

def mapa_heat(df,lc,oc,wcol,tile):
    dv=vdf(df,lc,oc); m=bmap(ctr(dv,lc,oc),tile)
    la=pd.to_numeric(dv[lc],errors="coerce"); lo2=pd.to_numeric(dv[oc],errors="coerce")
    if wcol and wcol in dv.columns:
        w=pd.to_numeric(dv[wcol],errors="coerce").fillna(0); mx=w.max()
        w=(w/mx).tolist() if mx>0 else w.tolist(); data=list(zip(la,lo2,w))
    else: data=list(zip(la,lo2))
    HeatMap(data,radius=18,blur=15,min_opacity=0.3,
            gradient={0.2:"blue",0.5:"lime",0.8:"orange",1.0:"red"}).add_to(m)
    return m

def mapa_clusters(df,lc,oc,ccol,tips,tile,scol):
    dv=vdf(df,lc,oc); m=bmap(ctr(dv,lc,oc),tile)
    has_c=ccol in dv.columns
    if scol and scol in dv.columns:
        vals=pd.to_numeric(dv[scol],errors="coerce").fillna(0); mx=vals.max()
        sizes=((vals/mx*14)+4).tolist() if mx>0 else [6]*len(dv)
    else: sizes=[6]*len(dv)
    for i,(_,r) in enumerate(dv.iterrows()):
        cid=int(r[ccol]) if has_c else 0; col=mcol(cid)
        p=([f"<b>Cluster</b>: {cid}"] if has_c else [])+\
          [f"<b>{c}</b>: {r.get(c,'')}" for c in tips if c in r.index]
        folium.CircleMarker([float(r[lc]),float(r[oc])],radius=sizes[i] if i<len(sizes) else 6,
            color=col,fill=True,fill_color=col,fill_opacity=0.78,
            tooltip=folium.Tooltip("<br>".join(p))).add_to(m)
    if has_c:
        uids=sorted(dv[ccol].dropna().unique())
        leg='<div style="position:fixed;bottom:30px;left:30px;background:#1a1f2e;padding:12px;border-radius:8px;border:1px solid #2a3045;z-index:9999;font-family:sans-serif;font-size:12px;color:#dde3f0;"><b>Clusters</b><br>'
        for c in uids[:20]:
            lbl="Ruido" if int(c)==-1 else f"Cluster {int(c)}"
            leg+=f'<span style="background:{mcol(int(c))};display:inline-block;width:12px;height:12px;border-radius:50%;margin-right:4px;"></span>{lbl}<br>'
        m.get_root().html.add_child(folium.Element(leg+"</div>"))
    return m

def mapa_mc(df,lc,oc,tips,tile):
    dv=vdf(df,lc,oc); m=bmap(ctr(dv,lc,oc),tile); mc2=MarkerCluster().add_to(m)
    for _,r in dv.iterrows():
        p=[f"<b>{c}</b>: {r.get(c,'')}" for c in tips if c in r.index]
        folium.Marker([float(r[lc]),float(r[oc])],
            popup=folium.Popup("<br>".join(p),max_width=300)).add_to(mc2)
    return m

def mapa_prop(df,lc,oc,scol,tips,tile,color):
    dv=vdf(df,lc,oc); m=bmap(ctr(dv,lc,oc),tile)
    vals=pd.to_numeric(dv[scol],errors="coerce").fillna(0); mx=vals.max()
    for i,(_,r) in enumerate(dv.iterrows()):
        v=float(vals.iloc[i]); rad=max(3.0,v/mx*30) if mx>0 else 5.0
        p=[f"<b>{scol}</b>: {v:,.0f}"]+[f"<b>{c}</b>: {r.get(c,'')}" for c in tips if c in r.index]
        folium.CircleMarker([float(r[lc]),float(r[oc])],radius=rad,color=color,
            fill=True,fill_color=color,fill_opacity=0.65,
            tooltip=folium.Tooltip("<br>".join(p))).add_to(m)
    return m


# ══════════════════════════════════════════════════════════════════════════════
# VALIDACIÓN GEOGRÁFICA — COLOMBIA
# ══════════════════════════════════════════════════════════════════════════════
# Bounding box Colombia (holgado para cubrir islas)
COL_LAT = (-4.5, 13.5)
COL_LON = (-82.0, -66.5)

def flag_out_of_colombia(df, lc, oc):
    """
    Añade columna 'coord_valida': True si la coord está dentro del bbox Colombia.
    Retorna df con la columna nueva y un dict de stats.
    """
    df = df.copy()
    lt = pd.to_numeric(df[lc], errors="coerce")
    ln = pd.to_numeric(df[oc], errors="coerce")
    in_col = (lt.between(*COL_LAT) & ln.between(*COL_LON) & lt.notna() & ln.notna())
    df["coord_valida"] = in_col
    total  = int(lt.notna().sum())
    inside = int(in_col.sum())
    return df, {"total_coords": total, "inside_colombia": inside,
                "outside": total - inside,
                "pct_ok": round(inside / max(total, 1) * 100, 1)}

def resumen_coordenadas(df, lc, oc):
    """Devuelve un DataFrame con las filas fuera de Colombia para revisión."""
    lt = pd.to_numeric(df[lc], errors="coerce")
    ln = pd.to_numeric(df[oc], errors="coerce")
    mask = lt.notna() & ln.notna() & ~(lt.between(*COL_LAT) & ln.between(*COL_LON))
    cols = [c for c in [lc, oc, "direccion", "dir_normalizada", "cliente_id"]
            if c in df.columns]
    return df[mask][cols].head(50)


def load_cache_from_csv(uploaded_file) -> tuple[dict, int]:
    """
    Carga un geo_cache.csv previamente exportado y lo fusiona con el caché actual.
    Retorna (cache_dict, n_nuevas_entradas).
    """
    try:
        df_c = pd.read_csv(uploaded_file, encoding="utf-8-sig")
        required = {"dir_normalizada", "latitud", "longitud"}
        if not required.issubset(df_c.columns):
            return {}, 0
        cache = st.session_state.setdefault("geo_cache", {})
        n_before = len(cache)
        for _, row in df_c.iterrows():
            addr = str(row["dir_normalizada"]).strip()
            if addr and addr != "nan" and addr not in cache:
                lt = None if pd.isna(row["latitud"])  else float(row["latitud"])
                ln = None if pd.isna(row["longitud"]) else float(row["longitud"])
                cache[addr] = (lt, ln)
        st.session_state["geo_cache"] = cache
        return cache, len(cache) - n_before
    except Exception:
        return {}, 0


def mapa_a_html(folium_map) -> bytes:
    """Convierte un mapa Folium a bytes HTML para descarga."""
    import io as _io
    buf = _io.BytesIO()
    folium_map.save(buf, close_file=False)
    return buf.getvalue()


def render_kpis(df):
    """Muestra KPIs compactos del dataset activo en 4 columnas."""
    lc = lat_col(df); kc = kg_col(df); pc = ped_col(df)
    k1,k2,k3,k4 = st.columns(4)
    k1.metric("Total filas", f"{len(df):,}")
    if lc:
        n_geo = int(pd.to_numeric(df[lc], errors="coerce").notna().sum())
        k2.metric("Geocodificados", f"{n_geo:,}", f"{round(n_geo/max(len(df),1)*100,1)}%")
    if kc:
        total_kg = pd.to_numeric(df[kc], errors="coerce").sum()
        k3.metric("Total KG", f"{total_kg:,.0f}")
    if pc:
        total_p = pd.to_numeric(df[pc], errors="coerce").sum()
        k4.metric("Total Pedidos", f"{total_p:,.0f}")

# ══════════════════════════════════════════════════════════════════════════════
# EXPORTACIÓN
# ══════════════════════════════════════════════════════════════════════════════
def to_csv(df): return df.to_csv(index=False).encode("utf-8-sig")
def to_xlsx(df,sheet="Datos"):
    b=io.BytesIO()
    with pd.ExcelWriter(b,engine="openpyxl") as w: df.to_excel(w,index=False,sheet_name=sheet[:31])
    return b.getvalue()

# ══════════════════════════════════════════════════════════════════════════════
# AUTOMAP
# ══════════════════════════════════════════════════════════════════════════════
def auto_map(df):
    low={c.lower():c for c in df.columns}
    H={"cliente_id":["id cliente","cliente_id","id_cliente","nit","cedula","customer","id"],
       "direccion":["dirección","direccion","address","dir","calle","domicilio","street"],
       "kg":["kg","kilogramos","kilos","peso","weight"],
       "num_pedidos":["pedidos","num_pedidos","orders","frecuencia","frequency"],
       "cantidad":["cantidad","quantity","qty","unidades","units"],
       "comprador":["comprador","buyer","vendedor","seller","nombre","name"],
       "fecha":["fecha","date","datetime","periodo","period"],
       "latitud":["latitud","lat","latitude"],"longitud":["longitud","lon","lng","longitude"]}
    m={}
    for canon,kws in H.items():
        for kw in kws:
            if kw in low and low[kw] not in m: m[low[kw]]=canon; break
    return m

# ══════════════════════════════════════════════════════════════════════════════
# CSS + INIT
# ══════════════════════════════════════════════════════════════════════════════
st.markdown("""
<style>
/* ── Tipografía ── */
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
html,body,[class*="css"]{font-family:'Inter',sans-serif;color:#e2e8f0;}

/* ── Fondo principal — slate muy oscuro pero no negro puro ── */
.stApp{background:#111827;}
.main .block-container{background:#111827;padding-top:1.5rem;}

/* ── Sidebar — slate medio ── */
section[data-testid="stSidebar"]{
    background:#1e293b;
    border-right:1px solid #334155;
}
section[data-testid="stSidebar"] *{color:#cbd5e1 !important;}
section[data-testid="stSidebar"] .stMarkdown h2,
section[data-testid="stSidebar"] .stMarkdown strong{color:#f1f5f9 !important;}

/* ── Header banner ── */
.gh{
    background:linear-gradient(135deg,#1e3a5f 0%,#1e293b 100%);
    border-left:4px solid #38bdf8;
    border-radius:12px;padding:1.2rem 1.8rem;margin-bottom:1.2rem;
}
.gh h1{color:#38bdf8;font-size:1.65rem;font-weight:700;margin:0;letter-spacing:-.02em;}
.gh p{color:#94a3b8;margin:.25rem 0 0;font-size:.875rem;}

/* ── Tabs ── */
.stTabs [data-baseweb="tab-list"]{
    gap:3px;background:#1e293b;
    border-radius:10px;padding:4px;border:1px solid #334155;
}
.stTabs [data-baseweb="tab"]{
    background:transparent;border-radius:7px;
    font-weight:500;font-size:.82rem;color:#94a3b8;
    padding:.4rem .9rem;border:none;
    transition:all .15s ease;
}
.stTabs [data-baseweb="tab"]:hover{background:#334155;color:#e2e8f0;}
.stTabs [aria-selected="true"]{
    background:#0ea5e9 !important;color:#fff !important;
    font-weight:600;box-shadow:0 2px 8px rgba(14,165,233,.35);
}

/* ── Métricas ── */
[data-testid="metric-container"]{
    background:#1e293b;border-radius:10px;
    padding:.9rem 1rem;border:1px solid #334155;
}
[data-testid="metric-container"] label{color:#94a3b8 !important;font-size:.78rem;}
[data-testid="metric-container"] [data-testid="stMetricValue"]{
    color:#f1f5f9 !important;font-size:1.5rem;font-weight:700;
}
[data-testid="metric-container"] [data-testid="stMetricDelta"]{color:#34d399 !important;}

/* ── Botones primarios ── */
.stButton>button[kind="primary"]{
    background:linear-gradient(135deg,#0ea5e9,#6366f1);
    color:#fff;font-weight:600;border:none;border-radius:8px;
    padding:.5rem 1.4rem;box-shadow:0 2px 8px rgba(14,165,233,.3);
    transition:all .2s ease;
}
.stButton>button[kind="primary"]:hover{
    transform:translateY(-1px);box-shadow:0 4px 14px rgba(14,165,233,.4);
}
.stButton>button:not([kind="primary"]){
    background:#1e293b;color:#cbd5e1;
    border:1px solid #334155;border-radius:8px;
    transition:all .15s ease;
}
.stButton>button:not([kind="primary"]):hover{
    background:#334155;border-color:#475569;color:#f1f5f9;
}

/* ── Inputs, selectbox, sliders ── */
.stTextInput>div>div>input,
.stSelectbox>div>div,
.stMultiselect>div>div{
    background:#1e293b !important;border-color:#334155 !important;
    color:#e2e8f0 !important;border-radius:8px !important;
}
.stSlider [data-baseweb="slider"] [role="slider"]{
    background:#0ea5e9 !important;border-color:#0ea5e9 !important;
}

/* ── Dataframe ── */
[data-testid="stDataFrame"]{border:1px solid #334155;border-radius:10px;overflow:hidden;}
.dvn-scroller{background:#1e293b;}

/* ── Expanders ── */
details{background:#1e293b;border:1px solid #334155;border-radius:10px;padding:.1rem .5rem;}
details summary{color:#94a3b8;font-weight:500;font-size:.9rem;}

/* ── Alertas info/warn/success ── */
[data-testid="stInfo"]{
    background:#1e3a5f;border-color:#38bdf8;color:#bae6fd;border-radius:8px;
}
[data-testid="stWarning"]{
    background:#2d1f00;border-color:#f59e0b;color:#fcd34d;border-radius:8px;
}
[data-testid="stSuccess"]{
    background:#052e16;border-color:#22c55e;color:#86efac;border-radius:8px;
}
[data-testid="stError"]{
    background:#2d0f0f;border-color:#ef4444;color:#fca5a5;border-radius:8px;
}

/* ── Progress bar ── */
[data-testid="stProgressBar"]>div>div{background:#0ea5e9 !important;}

/* ── File uploader ── */
[data-testid="stFileUploader"]{
    background:#1e293b;border:2px dashed #334155;
    border-radius:10px;padding:1rem;
}
[data-testid="stFileUploader"]:hover{border-color:#0ea5e9;}

/* ── Headers ── */
h1,h2,h3{color:#f1f5f9;font-weight:600;}
h2{color:#e2e8f0;font-size:1.35rem;border-bottom:1px solid #334155;padding-bottom:.5rem;}
p,li{color:#cbd5e1;}

/* ── Divisores ── */
hr{border-color:#334155;}

/* ── Download buttons ── */
[data-testid="stDownloadButton"]>button{
    background:#1e3a5f;color:#38bdf8;border:1px solid #38bdf8;
    border-radius:8px;font-weight:500;transition:all .15s;
}
[data-testid="stDownloadButton"]>button:hover{
    background:#38bdf8;color:#0f172a;
}

/* ── Radio buttons ── */
[data-testid="stRadio"] label{color:#cbd5e1 !important;}
[data-testid="stRadio"] [data-baseweb="radio"] div{border-color:#334155;}

/* ── Checkboxes ── */
[data-testid="stCheckbox"] label{color:#cbd5e1 !important;}
</style>""", unsafe_allow_html=True)

init_session()
ss = st.session_state

st.markdown('<div class="gh"><h1>🗺️ GeoSales Analytics</h1>'
            '<p>Normalización · Geocodificación Google Maps · Clusterización · Visualización geoespacial</p></div>',
            unsafe_allow_html=True)

# ══════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ══════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    st.markdown("## 🗺️ GeoSales"); st.caption("Análisis geoespacial de ventas"); st.divider()
    st.markdown("**📋 Pipeline**")
    for icon,lbl,flag in [("📂","Carga","step_uploaded"),("🔗","Mapeo","step_mapped"),
        ("📅","Filtro","step_filtered"),("✏️","Normalizar","step_normalized"),
        ("📍","Geocodificar","step_geocoded"),("🔵","Clusterizar","step_clustered")]:
        st.markdown(f"{'✅' if ss.get(flag) else '⬜'} {icon} {lbl}")
    st.divider(); st.markdown("**🔑 Google Maps API Key**"); st.caption("Solo en esta sesión.")
    api_in=st.text_input("Key","",type="password",placeholder="AIza...",label_visibility="collapsed",key="_ak")
    if api_in!=ss.get("google_api_key",""): st.session_state["google_api_key"]=api_in
    if api_in and st.button("Validar clave",use_container_width=True):
        ok,msg=validate_key(api_in); (st.success if ok else st.error)(msg)
    st.divider()
    # ── Estado de caché ───────────────────────────────────────────────────
    n_cache=len(ss.get("geo_cache",{}))
    if n_cache>0:
        free_equiv=min(n_cache,40000); cost_saved=round(n_cache/1000*5,2)
        st.markdown("**🗄️ Caché geocodificación**")
        st.caption(f"{n_cache:,} dirs · ahorro est. **${cost_saved:.2f} USD**")
    st.divider()
    dfx=get_active_df()
    if dfx is not None:
        st.markdown("**📊 Dataset activo**"); st.caption(f"{len(dfx):,} filas · {len(dfx.columns)} cols")
        lc0=lat_col(dfx)
        if lc0: st.caption(f"📍 {int(dfx[lc0].notna().sum()):,} con coords")
        if "cluster" in dfx.columns: st.caption(f"🔵 {dfx['cluster'].nunique()} clusters")
    st.divider()
    with st.expander("⚙️ Opciones"):
        if st.button("🔄 Resetear sesión",use_container_width=True):
            for k in list(ss.keys()): del st.session_state[k]
            init_session(); st.rerun()
        if st.button("🗑️ Limpiar caché geo",use_container_width=True):
            st.session_state["geo_cache"]={};st.success("Caché limpiado")

# ══════════════════════════════════════════════════════════════════════════════
# PESTAÑAS
# ══════════════════════════════════════════════════════════════════════════════
T=st.tabs(["📂 Carga","🔗 Mapeo","📅 Filtro","✏️ Normalizar",
           "📍 Geocodificar","🔵 Clusterizar","🗺️ Mapas","💾 Exportar"])


def _first_df(*keys):
    """Retorna el primer DataFrame no-None y no-vacío de session_state."""
    for k in keys:
        df = ss.get(k)
        if df is not None and isinstance(df, __import__('pandas').DataFrame) and not df.empty:
            return df
    return None

# ── TAB 1: CARGA ─────────────────────────────────────────────────────────────
with T[0]:
    st.header("📂 Carga y Validación")
    st.info("Sube tu archivo de ventas/pedidos en **CSV** o **XLSX**.")
    up=st.file_uploader("Selecciona el archivo",type=["csv","xlsx","xls"])
    if up is None:
        with st.expander("📋 Esquema mínimo esperado"):
            st.dataframe(pd.DataFrame({"id Cliente":[830061399,901391122],
                "dirección":["CL 75 27 28 , BOGOTA","KR 10 17 40 SUR , BOGOTA"],
                "KG":[155291.46,47900.65],"Pedidos":[548,525],"latitud":[None,None],"longitud":[None,None]}),
                use_container_width=True)
    else:
        raw=up.read()
        try:
            if up.name.lower().endswith(".csv"):
                df_l=None
                for enc in ("utf-8","utf-8-sig","latin-1"):
                    try: df_l=pd.read_csv(io.BytesIO(raw),sep=None,engine="python",encoding=enc); break
                    except: continue
                if df_l is None: st.error("No se pudo leer el CSV.")
            else: df_l=pd.read_excel(io.BytesIO(raw),engine="openpyxl")
        except Exception as e: st.error(f"❌ {e}"); df_l=None

        if df_l is not None and not df_l.empty:
            ltc=[c for c in df_l.columns if any(k in c.lower() for k in ("lat","latitude","latitud"))]
            lnc=[c for c in df_l.columns if any(k in c.lower() for k in ("lon","lng","longitude","longitud"))]
            clc=[c for c in df_l.columns if c.lower()=="cluster"]

            # ── Verificar que las columnas de coords tengan valores REALES ──
            # (puede existir la columna pero estar 100% vacía, como en bd.xlsx)
            def _has_real_coords(df, cols):
                """True solo si hay al menos 1 valor numérico válido en las columnas."""
                for c in cols:
                    vals = pd.to_numeric(df[c], errors="coerce")
                    if vals.notna().any():
                        return True
                return False

            col_lat_exists = bool(ltc)
            col_lon_exists = bool(lnc)
            coords_with_data = (col_lat_exists and col_lon_exists and
                                _has_real_coords(df_l, ltc) and
                                _has_real_coords(df_l, lnc))
            hc  = coords_with_data
            hcl = bool(clc)

            # Contar coords válidas para mostrar en KPI
            n_coords_valid = 0
            if col_lat_exists and col_lon_exists:
                lat_valid = pd.to_numeric(df_l[ltc[0]], errors="coerce").notna()
                lon_valid = pd.to_numeric(df_l[lnc[0]], errors="coerce").notna()
                n_coords_valid = int((lat_valid & lon_valid).sum())

            c1,c2,c3,c4=st.columns(4)
            c1.metric("Filas",f"{len(df_l):,}"); c2.metric("Columnas",len(df_l.columns))
            c3.metric("Nulos",f"{int(df_l.isnull().sum().sum()):,}")

            if col_lat_exists and col_lon_exists and not hc:
                # Columna existe pero está vacía — caso de tu bd.xlsx
                c4.metric("Coordenadas", "⚠️ Vacías",
                          help="La columna existe pero no tiene valores. Se geocodificará.")
                st.info("📍 Las columnas **latitud/longitud** existen pero están vacías. "                        "El sistema te llevará a geocodificar.")
            elif hc and hcl:
                c4.metric("Coordenadas", f"✅ {n_coords_valid:,}")
                st.success("✅ El archivo ya tiene **coordenadas y clusters** — puedes ir directo a **🗺️ Mapas**.")
            elif hc:
                c4.metric("Coordenadas", f"✅ {n_coords_valid:,}")
                st.success("✅ Coordenadas detectadas — puedes saltar la geocodificación.")
            else:
                c4.metric("Coordenadas", "❌ Sin coords",
                          help="No hay latitud/longitud. Deberás geocodificar.")
            st.dataframe(df_l.head(50),use_container_width=True,height=250)
            st.divider()
            btn_label = "🗺️ Cargar y ver Mapas directamente →" if (hc and hcl) else "✅ Confirmar carga → ir a Mapeo"
            if st.button(btn_label,use_container_width=True,type="primary"):
                am = auto_map(df_l)
                st.session_state.update({"df_original":df_l.copy(),"df_working":df_l.copy(),
                    "step_uploaded":True,"file_name":up.name,"col_mapping":am})
                # Solo saltar pasos si hay coordenadas CON DATOS REALES
                if hc:
                    dw=df_l.copy().rename(columns={k:v for k,v in am.items() if k in df_l.columns})
                    st.session_state.update({"df_working":dw,"step_mapped":True,
                        "df_geocoded":dw.copy(),"step_geocoded":True,"step_filtered":True,
                        "df_filtered":dw.copy(),"step_normalized":True,"df_normalized":dw.copy()})
                if hcl:
                    dw2=st.session_state["df_geocoded"].copy()
                    st.session_state.update({"df_clustered":dw2,"step_clustered":True})
                if hc and hcl:
                    st.success("✅ Listo. Ve a la pestaña **🗺️ Mapas** para visualizar.")
                else:
                    st.success("✅ Cargado. Ve a **🔗 Mapeo**."); st.balloons()

# ── TAB 2: MAPEO ─────────────────────────────────────────────────────────────
with T[1]:
    st.header("🔗 Mapeo de Columnas")
    if not ss.get("step_uploaded"): st.warning("⚠️ Primero carga un archivo.")
    else:
        df_o=ss["df_original"]; opts=["— Sin mapear —"]+list(df_o.columns)
        cur=ss.get("col_mapping",{}); inv={v:k for k,v in cur.items()}
        st.info("Asocia cada columna con su concepto. Si ya tienes coords puedes saltar geocodificación.")
        CDEF={"cliente_id":("🆔 ID Cliente",True),"direccion":("📍 Dirección",True),
              "kg":("⚖️ KG",False),"num_pedidos":("📦 N° Pedidos",False),
              "cantidad":("🔢 Cantidad",False),"comprador":("👤 Comprador",False),
              "fecha":("📅 Fecha",False),"latitud":("🌐 Latitud",False),"longitud":("🌐 Longitud",False)}
        nm={}; ks=list(CDEF.keys()); mid=len(ks)//2; ca,cb=st.columns(2)
        for canon in ks:
            lb,rq=CDEF[canon]; mk=" *" if rq else ""
            cr2=inv.get(canon,"— Sin mapear —"); idx=opts.index(cr2) if cr2 in opts else 0
            cont=ca if ks.index(canon)<mid else cb
            ch=cont.selectbox(f"{lb}{mk}",opts,index=idx,key=f"m_{canon}")
            if ch!="— Sin mapear —": nm[ch]=canon
        st.divider(); mv=set(nm.values()); hd="direccion" in mv; hc2=("latitud" in mv and "longitud" in mv)
        if hc2: st.success("✅ Coords mapeadas — puedes saltar geocodificación.")
        elif hd: st.info("📍 Dirección mapeada.")
        else: st.warning("⚠️ Mapea al menos Dirección o Latitud+Longitud.")
        if st.button("✅ Confirmar mapeo →",use_container_width=True,type="primary",disabled=not(hd or hc2)):
            dw=ss["df_original"].copy().rename(columns={k:v for k,v in nm.items() if k in ss["df_original"].columns})
            st.session_state.update({"col_mapping":nm,"df_working":dw,"step_mapped":True})
            invalidate_from("normalize"); st.success("✅ Mapeo guardado.")

# ── TAB 3: FILTRO ─────────────────────────────────────────────────────────────
with T[2]:
    st.header("📅 Filtro Temporal")
    if not ss.get("step_mapped"): st.warning("⚠️ Completa el Mapeo.")
    else:
        df_ft=_first_df("df_working","df_original")
        fc="fecha" if "fecha" in df_ft.columns else None
        if fc is None:
            st.info("No se mapeó columna de fecha.")
            if st.button("➡️ Continuar sin filtro",use_container_width=True,type="primary"):
                st.session_state.update({"df_filtered":df_ft.copy(),"step_filtered":True}); st.success("OK.")
        else:
            df_ft2=df_ft.copy(); df_ft2[fc]=pd.to_datetime(df_ft2[fc],errors="coerce")
            iv=int(df_ft2[fc].isna().sum())
            if iv: st.warning(f"⚠️ {iv:,} fechas inválidas excluidas.")
            df_ft2=df_ft2.dropna(subset=[fc])
            if not df_ft2.empty:
                md=df_ft2[fc].min().date(); mxd=df_ft2[fc].max().date()
                st.caption(f"Rango: **{md}** → **{mxd}**")
                pr=st.radio("Período",["Últimos 30 días","Últimos 3 meses","Últimos 6 meses","Último año","Rango personalizado","Sin filtro"],horizontal=True)
                td=mxd
                if   pr=="Últimos 30 días":  d_from,d_to=td-timedelta(30),td
                elif pr=="Últimos 3 meses":  d_from,d_to=td-timedelta(90),td
                elif pr=="Últimos 6 meses":  d_from,d_to=td-timedelta(180),td
                elif pr=="Último año":        d_from,d_to=td-timedelta(365),td
                elif pr=="Rango personalizado":
                    rc1,rc2=st.columns(2)
                    d_from=rc1.date_input("Desde",md,min_value=md,max_value=mxd)
                    d_to  =rc2.date_input("Hasta",mxd,min_value=md,max_value=mxd)
                else: d_from,d_to=md,mxd
                mask=df_ft2[fc].dt.date.between(d_from,d_to); flt=df_ft2[mask]

                # ── Filtro adicional por segmento ────────────────────────
                st.divider()
                st.markdown("**Filtros adicionales** *(opcional)*")
                fa1,fa2=st.columns(2)

                # Filtro por comprador
                comp_col="comprador" if "comprador" in flt.columns else None
                if comp_col and flt[comp_col].notna().any():
                    all_compr=sorted(flt[comp_col].dropna().unique().tolist())
                    sel_comp=fa1.multiselect(f"Filtrar por {comp_col}",all_compr,
                                             placeholder="Todos (sin filtrar)",key="f_comp")
                    if sel_comp: flt=flt[flt[comp_col].isin(sel_comp)]

                # Filtro por rango de KG
                kgc_ft=kg_col(flt)
                if kgc_ft and pd.to_numeric(flt[kgc_ft],errors="coerce").notna().any():
                    kg_num=pd.to_numeric(flt[kgc_ft],errors="coerce").dropna()
                    kg_min,kg_max=float(kg_num.min()),float(kg_num.max())
                    if kg_max>kg_min:
                        rng=fa2.slider(f"Rango de {kgc_ft}",kg_min,kg_max,(kg_min,kg_max),
                                       key="f_kg",format="%.0f")
                        kg_mask=pd.to_numeric(flt[kgc_ft],errors="coerce").between(*rng)
                        flt=flt[kg_mask]

                st.metric("Filas que cumplen todos los filtros",f"{len(flt):,}"); st.divider()
                if st.button("✅ Aplicar filtros →",use_container_width=True,type="primary"):
                    st.session_state.update({"df_filtered":flt.copy(),"step_filtered":True})
                    invalidate_from("normalize"); st.success(f"✅ {len(flt):,} filas aplicadas.")

# ── TAB 4: NORMALIZAR ─────────────────────────────────────────────────────────
with T[3]:
    st.header("✏️ Normalización de Direcciones")
    if not ss.get("step_mapped"): st.warning("⚠️ Completa el Mapeo.")
    else:
        df_nm=pipeline_df()
        if df_nm is None: st.error("No hay datos.")
        else:
            ac="direccion" if "direccion" in df_nm.columns else None
            if ac is None:
                cands=[c for c in df_nm.columns if any(k in c.lower() for k in ("dir","address","calle"))]
                ac=st.selectbox("Columna de dirección",cands or df_nm.columns.tolist()) if cands else None
            if ac is None: st.error("No se encontró columna de dirección.")
            else:
                st.info("Expande abreviaturas colombianas (**CL→Calle, KR→Carrera…**) para mejorar Google Maps.")
                lc1=lat_col(df_nm); oc1=lon_col(df_nm)
                if lc1 and oc1 and df_nm[lc1].notna().sum()>0:
                    st.success("✅ Ya tiene coordenadas válidas.")
                    if st.button("⏩ Saltar normalización",use_container_width=True):
                        st.session_state.update({"df_normalized":df_nm.copy(),"step_normalized":True}); st.info("Omitido.")
                    st.divider()
                n1,n2=st.columns(2)
                ctry=n1.text_input("Sufijo país","Colombia"); ocol=n2.text_input("Columna normalizada","dir_normalizada")
                st_=addr_stats(df_nm,ac)
                m1,m2,m3=st.columns(3)
                m1.metric("Total",f"{st_['total']:,}"); m2.metric("Vacías",f"{st_['empty']:,}"); m3.metric("Con número",f"{st_['with_num']:,} ({st_['pct_ok']}%)")
                with st.expander("👁️ Preview"):
                    sp=df_nm[[ac]].dropna().head(10).copy(); sp["normalizada"]=sp[ac].apply(lambda a:norm_addr(str(a),ctry)); st.dataframe(sp,use_container_width=True)
                st.divider()
                if st.button("🚀 Normalizar",use_container_width=True,type="primary"):
                    with st.spinner("Normalizando…"): df_n2=norm_col(df_nm,ac,ctry,ocol)
                    st.session_state.update({"df_normalized":df_n2,"step_normalized":True}); invalidate_from("geocode")
                    st.success(f"✅ {len(df_n2):,} filas. Columna **{ocol}** añadida.")
                    st.dataframe(df_n2[[ac,ocol]].head(15),use_container_width=True)

# ── TAB 5: GEOCODIFICAR ───────────────────────────────────────────────────────
with T[4]:
    st.header("📍 Geocodificación")
    if not ss.get("step_mapped"): st.warning("⚠️ Completa el Mapeo.")
    else:
        df_gc=pipeline_df()
        if df_gc is None: st.error("No hay datos.")
        else:
            lc2=lat_col(df_gc); oc2=lon_col(df_gc); ic=check_coords(df_gc,lc2 or "",oc2 or "")
            if ic["has"] and ic["pct"]>=50:
                st.success(f"✅ Ya tiene coords: **{ic['valid']:,}/{ic['total']:,}** ({ic['pct']}%).")
                if st.button("⏩ Usar coords existentes",use_container_width=True,type="primary"):
                    st.session_state.update({"df_geocoded":df_gc.copy(),"step_geocoded":True}); st.success("✅ Ve a Clusterizar o Mapas.")
            if not ss.get("step_geocoded"):
                st.divider()
                # ── Cargar caché previa ────────────────────────────────────
                with st.expander("🗄️ Cargar caché de geocodificación previa (ahorra costos)"):
                    st.caption("Si exportaste un `geo_cache.csv` en una sesión anterior, súbelo aquí para "
                               "reutilizar las coordenadas ya obtenidas sin gastar más cuota de API.")
                    cache_up=st.file_uploader("Subir geo_cache.csv",type=["csv"],key="cache_upload")
                    if cache_up is not None:
                        _, n_new = load_cache_from_csv(cache_up)
                        total_c  = len(ss.get("geo_cache",{}))
                        if n_new > 0:
                            st.success(f"✅ {n_new:,} entradas nuevas cargadas. Caché total: {total_c:,} direcciones.")
                        else:
                            st.info(f"ℹ️ Todas las entradas ya estaban en caché ({total_c:,} en total).")
                st.subheader("⚙️ Configurar geocodificación")
                akey=ss.get("google_api_key","")
                if not akey: st.warning("⚠️ Ingresa tu API Key en el panel lateral.")
                else:
                    ac2=[c for c in df_gc.columns if c in ("dir_normalizada","direccion","address")]
                    if not ac2: ac2=df_gc.select_dtypes(include="object").columns.tolist()
                    g1,g2=st.columns(2)
                    acg=g1.selectbox("Columna dirección",ac2 or df_gc.columns.tolist())
                    with g2:
                        dlg=st.slider("Delay entre llamadas (s)",0.02,2.0,0.05,0.01,
                                      help="Más bajo = más rápido, mayor riesgo de límite de cuota")
                        modo_geo=st.radio("Modo",["🔄 Secuencial","⚡ Paralelo (más rápido)"],
                                          horizontal=True,key="geo_mode",
                                          help="Paralelo usa 5 workers simultáneos — recomendado para >500 dirs")
                        n_workers=5 if "Paralelo" in modo_geo else 1
                    if acg not in df_gc.columns:
                        st.error(f"La columna '{acg}' no existe. Revisa el mapeo.")
                    else:
                        nu=int(df_gc[acg].dropna().astype(str).nunique()); ca2=len(ss.get("geo_cache",{})); pe=max(0,nu-ca2)
                        u1,u2,u3=st.columns(3); u1.metric("Dirs. únicas",f"{nu:,}"); u2.metric("En caché",f"{ca2:,}"); u3.metric("Llamadas est.",f"{pe:,}")
                        if pe>0: st.caption(f"💰 Costo est.: **${pe*0.005:.2f} USD**")
                    st.divider(); rb,sb=st.columns([4,1])
                    runb=rb.button("🚀 Iniciar geocodificación",use_container_width=True,type="primary")
                    if sb.button("⛔ Detener",use_container_width=True): st.session_state["stop_geocoding"]=True
                    if runb:
                        st.session_state["stop_geocoding"]=False; bar=st.progress(0); stxt=st.empty()
                        def _p(d,t): bar.progress(d/max(t,1)); stxt.caption(f"⏳ {d}/{t}")
                        with st.spinner("Geocodificando…"):
                            try:
                                if n_workers > 1:
                                    df_g2,gs=geo_df_async(df_gc,acg,akey,
                                                          max_workers=n_workers,delay=dlg,cb=_p)
                                else:
                                    df_g2,gs=geo_df(df_gc,acg,akey,dlg,_p)
                            except Exception as e: st.error(f"❌ {e}"); df_g2=None; gs={}
                        if df_g2 is not None:
                            bar.progress(1.0); stxt.empty()
                            st.session_state.update({"df_geocoded":df_g2,"step_geocoded":True}); invalidate_from("cluster")
                            r1,r2,r3,r4=st.columns(4)
                            r1.metric("✅ Exitosas",f"{gs['ok']:,}"); r2.metric("⚠️ Sin resultado",f"{gs['zero']:,}")
                            r3.metric("❌ Errores",f"{gs['errors']:,}"); r4.metric("📍 %",f"{gs['pct']}%")
                            if gs.get("quota"): st.warning("⚠️ Cuota agotada. Progreso guardado en caché.")
                            pc=[c for c in [acg,"latitud","longitud"] if c in df_g2.columns]
                            st.dataframe(df_g2[pc].head(20),use_container_width=True)
                            # ── Validar coords en Colombia ────────────────
                            if "latitud" in df_g2.columns and "longitud" in df_g2.columns:
                                df_g2, geo_stats = flag_out_of_colombia(df_g2,"latitud","longitud")
                                st.session_state["df_geocoded"] = df_g2
                                if geo_stats["outside"] > 0:
                                    st.warning(
                                        f"⚠️ **{geo_stats['outside']:,} coordenadas fuera de Colombia** "
                                        f"({100-geo_stats['pct_ok']:.1f}%). Revisa la tabla de sospechosas."
                                    )
                                    with st.expander("🔍 Corregir coordenadas sospechosas"):
                                        df_susp=resumen_coordenadas(df_g2,"latitud","longitud").copy()
                                        st.caption(
                                            "Edita directamente las celdas de **latitud** y **longitud** "
                                            "para corregir los registros que cayeron fuera de Colombia. "
                                            "Luego haz clic en **Aplicar correcciones**."
                                        )
                                        # Tabla editable
                                        edited=st.data_editor(
                                            df_susp,
                                            column_config={
                                                "latitud":  st.column_config.NumberColumn("Latitud",  format="%.6f", min_value=-90,  max_value=90),
                                                "longitud": st.column_config.NumberColumn("Longitud", format="%.6f", min_value=-180, max_value=180),
                                            },
                                            use_container_width=True,
                                            hide_index=True,
                                            key="edit_coords",
                                        )
                                        if st.button("💾 Aplicar correcciones",key="apply_coords"):
                                            df_fix=df_g2.copy()
                                            # Buscar filas por dirección normalizada si existe, o índice
                                            id_col=next((c for c in ["dir_normalizada","direccion","cliente_id"]
                                                         if c in df_fix.columns and c in edited.columns),None)
                                            if id_col:
                                                for _,row in edited.iterrows():
                                                    mask_fix=df_fix[id_col]==row[id_col]
                                                    if mask_fix.any():
                                                        df_fix.loc[mask_fix,"latitud"]  = row["latitud"]
                                                        df_fix.loc[mask_fix,"longitud"] = row["longitud"]
                                                        # Actualizar caché también
                                                        addr_key=str(row.get("dir_normalizada",row.get("direccion","")))
                                                        if addr_key:
                                                            st.session_state["geo_cache"][addr_key]=(row["latitud"],row["longitud"])
                                            df_fix,_=flag_out_of_colombia(df_fix,"latitud","longitud")
                                            st.session_state["df_geocoded"]=df_fix
                                            remaining=int((~df_fix["coord_valida"]).sum())
                                            if remaining==0:
                                                st.success("✅ Todas las coords ahora están dentro de Colombia.")
                                            else:
                                                st.info(f"Quedan {remaining} coords fuera. Puedes seguir corrigiendo.")
                                else:
                                    st.success(f"✅ Todas las coordenadas están dentro de Colombia ({geo_stats['pct_ok']}%).")
            if ss.get("step_geocoded") and ss.get("df_geocoded") is not None:
                dg3=ss["df_geocoded"]
                n2=int(dg3["latitud"].notna().sum()) if "latitud" in dg3.columns else 0
                st.success(f"✅ Geocodificado: **{n2:,}** registros con coordenadas.")
                if "coord_valida" in dg3.columns:
                    out_=int((~dg3["coord_valida"]).sum())
                    if out_>0: st.warning(f"⚠️ {out_:,} coords fuera de Colombia.")
                    else: st.info("✅ Todas las coords dentro de Colombia.")

# ── TAB 6: CLUSTERIZAR ────────────────────────────────────────────────────────
with T[5]:
    st.header("🔵 Clusterización Geográfica")
    if not ss.get("step_mapped"): st.warning("⚠️ Completa el Mapeo.")
    else:
        df_cl=pipeline_df()
        if df_cl is None: st.error("No hay datos.")
        else:
            lc3=lat_col(df_cl); oc3=lon_col(df_cl)
            if not lc3 or not oc3: st.warning("⚠️ No hay coords. Geocodifica primero.")
            else:
                vn=int(pd.to_numeric(df_cl[lc3],errors="coerce").notna().sum())
                if vn<5: st.error(f"Solo {vn} puntos. Necesitas ≥5.")
                else:
                    st.caption(f"{len(df_cl):,} filas · **{vn:,}** con coords")
                    nc=df_cl.select_dtypes(include="number").columns.tolist(); kgd=kg_col(df_cl); pedd=ped_col(df_cl)
                    st.subheader("① Método")
                    meth=st.radio("Método",list(METHODS.keys()),format_func=lambda m:METHODS[m].label,horizontal=True)
                    inf2=METHODS[meth]
                    with st.expander(f"ℹ️ {inf2.label}"):
                        x1,x2=st.columns(2)
                        x1.markdown("**✅ Ventajas**\n"+"\n".join(f"- {p}" for p in inf2.pros))
                        x2.markdown("**⚠️ Limitaciones**\n"+"\n".join(f"- {c}" for c in inf2.cons))
                        st.caption(inf2.hint)
                    st.subheader("② Parámetros"); pars={}
                    if meth in ("KMeans","MiniBatchKMeans"):
                        p1,p2=st.columns(2); pars["n_clusters"]=p1.slider("Clusters (K)",2,50,8)
                        if meth=="MiniBatchKMeans": pars["batch_size"]=p2.slider("Batch size",256,4096,1024,128)
                    elif meth=="DBSCAN":
                        p1,p2=st.columns(2); pars["eps"]=p1.slider("eps",0.001,0.10,0.01,0.001,format="%.3f"); pars["min_samples"]=p2.slider("Min muestras",2,30,5)
                    st.subheader("③ Variable de gravedad (ponderación)")
                    st.info(
                        "Define qué columna numérica **da más importancia** a cada punto al formar clusters. "
                        "Un cliente con 50.000 kg atraerá más al centroide que uno con 100 kg."
                    )
                    wt=st.radio("Gravedad",list(WEIGHTS.keys()),
                                format_func=lambda k:WEIGHTS[k],horizontal=True,key="wt_main")

                    # Detectar columna de cantidad
                    cantd=next((c for c in df_cl.columns if c.lower() in
                                ("cantidad","quantity","qty","unidades","units")),None)

                    kgs=peds=cants=None
                    needs_kg   = wt in ("kg","kg_ped","kg_cant","todo")
                    needs_ped  = wt in ("pedidos","kg_ped","ped_cant","todo")
                    needs_cant = wt in ("cantidad","kg_cant","ped_cant","todo")

                    sel_cols = st.columns(
                        [1]*sum([needs_kg, needs_ped, needs_cant]) or [1]
                    )
                    col_idx = 0
                    if needs_kg:
                        di=nc.index(kgd) if kgd in nc else 0
                        kgs=sel_cols[col_idx].selectbox("⚖️ Columna KG",nc,index=di,key="sk")
                        col_idx+=1
                    if needs_ped:
                        di=nc.index(pedd) if pedd in nc else 0
                        peds=sel_cols[col_idx].selectbox("📦 Columna Pedidos",nc,index=di,key="sp")
                        col_idx+=1
                    if needs_cant:
                        di=nc.index(cantd) if cantd in nc else 0
                        cants=sel_cols[col_idx].selectbox("🔢 Columna Cantidad",nc,index=di,key="sc")

                    st.divider()
                    if st.button("🚀 Clusterizar",use_container_width=True,type="primary"):
                        with st.spinner(f"Ejecutando {inf2.label}…"):
                            dcl,cst=do_cluster(df_cl,lc3,oc3,meth,pars,wt,kgs,peds,cants)
                        if "error" in cst: st.error(f"❌ {cst['error']}")
                        else:
                            st.session_state.update({"df_clustered":dcl,"step_clustered":True})
                            k1,k2,k3,k4=st.columns(4)
                            k1.metric("Clusters",cst["n_clusters"]); k2.metric("Puntos",f"{cst['n_points']:,}")
                            k3.metric("Ruido",f"{cst['noise']:,}"); k4.metric("% sin cluster",f"{cst['pct_noise']}%")
                            st.dataframe(cst["summary"],use_container_width=True)
                            st.success("✅ Ve a **🗺️ Mapas** o **💾 Exportar**.")

                    # ── Comparador de escenarios K (Método del Codo) ────────
                    if meth in ("KMeans","MiniBatchKMeans"):
                        st.divider()
                        with st.expander("📊 Comparador de escenarios K — Método del Codo"):
                            st.caption(
                                "Ejecuta KMeans para varios valores de K y compara la **Inercia** "
                                "(cuánto varían los puntos dentro de cada cluster — menor es mejor) "
                                "y el **Silhouette Score** (qué tan bien separados están los clusters — "
                                "más alto es mejor, máximo 1.0). Busca el 'codo' en la curva de inercia."
                            )
                            k_rng=st.slider("Rango de K a evaluar",2,30,(2,min(15,vn//10+2)),key="codo_k")
                            k_list=list(range(k_rng[0],k_rng[1]+1))
                            wt_codo=st.radio("Gravedad",list(WEIGHTS.keys()),
                                             format_func=lambda x:WEIGHTS[x],horizontal=True,key="codo_w")
                            kgs_c=peds_c=cants_c=None
                            nk_kg   = wt_codo in ("kg","kg_ped","kg_cant","todo")
                            nk_ped  = wt_codo in ("pedidos","kg_ped","ped_cant","todo")
                            nk_cant = wt_codo in ("cantidad","kg_cant","ped_cant","todo")
                            cc=st.columns([1]*sum([nk_kg,nk_ped,nk_cant]) or [1]); ci=0
                            if nk_kg and nc:
                                di_c=nc.index(kgd) if kgd in nc else 0
                                kgs_c=cc[ci].selectbox("⚖️ KG",nc,index=di_c,key="codo_kg"); ci+=1
                            if nk_ped and nc:
                                di_c=nc.index(pedd) if pedd in nc else 0
                                peds_c=cc[ci].selectbox("📦 Pedidos",nc,index=di_c,key="codo_ped"); ci+=1
                            if nk_cant and nc:
                                cantd_c=next((c for c in df_cl.columns if c.lower() in ("cantidad","qty","units")),None)
                                di_c=nc.index(cantd_c) if cantd_c in nc else 0
                                cants_c=cc[ci].selectbox("🔢 Cantidad",nc,index=di_c,key="codo_cant")
                            if st.button("▶️ Calcular comparación",use_container_width=True,key="codo_btn"):
                                with st.spinner(f"Evaluando K={k_list[0]}…{k_list[-1]}…"):
                                    df_codo=comparar_k(df_cl,lc3,oc3,k_list,wt_codo,kgs_c,peds_c,cants_c)
                                st.session_state["codo_df"]=df_codo
                            if "codo_df" in ss and ss["codo_df"] is not None:
                                df_c=ss["codo_df"]
                                st.dataframe(df_c,use_container_width=True,hide_index=True)
                                # Gráfico
                                try:
                                    import plotly.graph_objects as go
                                    fig=go.Figure()
                                    fig.add_trace(go.Scatter(x=df_c["K"],y=df_c["Inercia"],
                                        mode="lines+markers",name="Inercia",line=dict(color="#00c9a7",width=2)))
                                    fig.update_layout(
                                        title="Método del Codo — Inercia por K",
                                        xaxis_title="K (número de clusters)",
                                        yaxis_title="Inercia (WCSS)",
                                        plot_bgcolor="#1a1f2e",paper_bgcolor="#0f1117",
                                        font=dict(color="#dde3f0"),height=320,
                                    )
                                    st.plotly_chart(fig,use_container_width=True)
                                    if df_c["Silhouette"].notna().any():
                                        fig2=go.Figure()
                                        fig2.add_trace(go.Bar(x=df_c["K"],y=df_c["Silhouette"],
                                            marker_color=["#00c9a7" if v==df_c["Silhouette"].max() else "#2196f3"
                                                          for v in df_c["Silhouette"]],name="Silhouette"))
                                        fig2.update_layout(
                                            title="Silhouette Score por K (más alto = mejor separación)",
                                            xaxis_title="K",yaxis_title="Silhouette",
                                            plot_bgcolor="#1a1f2e",paper_bgcolor="#0f1117",
                                            font=dict(color="#dde3f0"),height=280,
                                        )
                                        st.plotly_chart(fig2,use_container_width=True)
                                        best_k=int(df_c.loc[df_c["Silhouette"].idxmax(),"K"])
                                        st.success(f"💡 El **K={best_k}** tiene el mejor Silhouette Score "
                                                   f"({df_c['Silhouette'].max():.4f}).")
                                except ImportError:
                                    st.info("Instala plotly para ver los gráficos: `pip install plotly`")

# ── TAB 7: MAPAS ──────────────────────────────────────────────────────────────
with T[6]:
    st.header("🗺️ Visualización en Mapas")
    if not _FOLIUM: st.error("❌ Instala `folium` y `streamlit-folium`.")
    elif not ss.get("step_mapped"): st.warning("⚠️ Completa el Mapeo.")
    else:
        df_mp=_first_df("df_clustered","df_geocoded","df_normalized","df_working")
        if df_mp is None: st.warning("⚠️ No hay datos disponibles.")
        else:
            lc4=lat_col(df_mp); oc4=lon_col(df_mp)
            if not lc4 or not oc4: st.warning("⚠️ Sin coords. Geocodifica primero.")
            else:
                vm=(pd.to_numeric(df_mp[lc4],errors="coerce").notna()&pd.to_numeric(df_mp[oc4],errors="coerce").notna())
                df_m=df_mp[vm].copy()
                if df_m.empty: st.error("No hay puntos con coords.")
                else:
                    st.caption(f"**{len(df_m):,}** puntos disponibles")
                    render_kpis(df_m)
                    mc1,mc2=st.columns([2,1])
                    mode=mc1.selectbox("Visualización",["Puntos simples","Heatmap (densidad)","Clusters coloreados","Agrupación automática (MarkerCluster)","Círculos proporcionales"])
                    tile=mc2.selectbox("Mapa base",list(TILES.keys()))
                    nm2=df_m.select_dtypes(include="number").columns.tolist(); all_c=df_m.columns.tolist(); hcl="cluster" in df_m.columns
                    kgm=kg_col(df_m); pedm=ped_col(df_m); wcm=scm=ccm=None; colm="#00c9a7"
                    with st.expander("⚙️ Opciones"):
                        tips=st.multiselect("Tooltip",all_c,default=[c for c in all_c if c.lower() in ("cliente_id","kg","num_pedidos")][:3])
                        if mode=="Heatmap (densidad)":
                            w2=st.selectbox("Intensidad",["— Sin peso —"]+nm2); wcm=None if w2.startswith("—") else w2
                        elif mode=="Clusters coloreados":
                            ccm="cluster" if hcl else st.selectbox("Col cluster",all_c)
                            s2=st.selectbox("Tamaño proporcional",["— Sin tamaño —"]+nm2); scm=None if s2.startswith("—") else s2
                        elif mode=="Círculos proporcionales":
                            di=nm2.index(kgm) if kgm in nm2 else 0; scm=st.selectbox("Variable tamaño",nm2,index=di) if nm2 else None; colm=st.color_picker("Color","#ffd166")
                        elif mode=="Puntos simples": colm=st.color_picker("Color","#00c9a7")
                    mxp=st.slider("Máx. puntos",500,min(50_000,len(df_m)),min(10_000,len(df_m)),500)
                    if len(df_m)>mxp: df_m=df_m.sample(n=mxp,random_state=42); st.caption(f"⚡ Muestra de {mxp:,} puntos.")
                    with st.spinner("Generando mapa…"):
                        try:
                            if   mode=="Puntos simples":                        mo=mapa_puntos(df_m,lc4,oc4,tips,tile,colm)
                            elif mode=="Heatmap (densidad)":                    mo=mapa_heat(df_m,lc4,oc4,wcm,tile)
                            elif mode=="Clusters coloreados":                   mo=mapa_clusters(df_m,lc4,oc4,ccm if ccm and ccm in df_m.columns else df_m.columns[0],tips,tile,scm)
                            elif mode=="Agrupación automática (MarkerCluster)": mo=mapa_mc(df_m,lc4,oc4,tips,tile)
                            elif mode=="Círculos proporcionales":
                                if not scm: st.warning("Selecciona variable de tamaño."); mo=None
                                else: mo=mapa_prop(df_m,lc4,oc4,scm,tips,tile,colm)
                            if mo:
                                st_folium(mo,width="100%",height=540,returned_objects=[])
                                # ── Exportar mapa como HTML ──────────────
                                try:
                                    html_bytes = mapa_a_html(mo)
                                    map_fname  = f"mapa_{mode.split()[0].lower()}_{ss.get('file_name','datos').rsplit('.',1)[0]}.html"
                                    st.download_button(
                                        "⬇️ Descargar mapa interactivo (.html)",
                                        data=html_bytes,
                                        file_name=map_fname,
                                        mime="text/html",
                                        help="Abre el archivo en cualquier navegador. No necesita conexión a internet.",
                                    )
                                except Exception:
                                    pass  # No interrumpir si falla la exportación
                        except Exception as e: st.error(f"❌ {e}")
                    st.divider(); s1,s2,s3,s4=st.columns(4); s1.metric("Puntos",f"{len(df_m):,}")
                    if kgm and kgm in df_m.columns: s2.metric("Total KG",f"{pd.to_numeric(df_m[kgm],errors='coerce').sum():,.0f}")
                    if pedm and pedm in df_m.columns: s3.metric("Total Pedidos",f"{pd.to_numeric(df_m[pedm],errors='coerce').sum():,.0f}")
                    if hcl: s4.metric("Clusters",int(df_m["cluster"].nunique()))
                    if hcl:
                        with st.expander("📋 Tabla por cluster"):
                            ag2={}
                            if kgm  and kgm  in df_m.columns: ag2[kgm] ="sum"
                            if pedm and pedm in df_m.columns: ag2[pedm]="sum"
                            cn2=df_m.groupby("cluster").size().rename("n_puntos")
                            sm2=df_m.groupby("cluster").agg(ag2).join(cn2).reset_index() if ag2 else cn2.reset_index()
                            st.dataframe(sm2.sort_values("cluster"),use_container_width=True)

# ── TAB 8: EXPORTAR ───────────────────────────────────────────────────────────
with T[7]:
    st.header("💾 Exportar Resultados")
    df_ex=get_active_df()
    if df_ex is None: st.warning("⚠️ No hay datos para exportar.")
    else:
        dl=[l for f,l in [("step_uploaded","✅ Cargado"),("step_mapped","✅ Mapeado"),("step_filtered","✅ Filtrado"),("step_normalized","✅ Normalizado"),("step_geocoded","✅ Geocodificado"),("step_clustered","✅ Clusterizado")] if ss.get(f)]
        st.caption("  ·  ".join(dl)); st.caption(f"{len(df_ex):,} filas · {len(df_ex.columns)} cols")
        base=ss.get("file_name","resultado").rsplit(".",1)[0]
        st.subheader("📁 Dataset completo")
        e1,e2=st.columns(2)
        e1.download_button("⬇️ CSV",to_csv(df_ex),f"{base}_procesado.csv","text/csv",use_container_width=True)
        e2.download_button("⬇️ XLSX",to_xlsx(df_ex),f"{base}_procesado.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",use_container_width=True)
        lce=lat_col(df_ex)
        if ss.get("step_geocoded") and lce:
            st.divider(); st.subheader("📍 Solo geocodificados")
            dge=df_ex[df_ex[lce].notna()]; st.caption(f"{len(dge):,} con coords")
            st.download_button("⬇️ Geocodificados CSV",to_csv(dge),f"{base}_geocodificados.csv","text/csv",use_container_width=True)
        if ss.get("step_clustered") and "cluster" in df_ex.columns:
            st.divider(); st.subheader("📊 Resumen por cluster")
            kge=kg_col(df_ex); pede=ped_col(df_ex); ag3={}
            if kge  and kge  in df_ex.columns: ag3[kge] =["sum","mean"]
            if pede and pede in df_ex.columns: ag3[pede]=["sum","mean"]
            cn3=df_ex.groupby("cluster").size().rename("n_puntos")
            if ag3:
                sm3=df_ex.groupby("cluster").agg(ag3); sm3.columns=["_".join(c) for c in sm3.columns]; sm3=sm3.join(cn3).reset_index()
            else: sm3=cn3.reset_index()
            st.dataframe(sm3,use_container_width=True)
            f1,f2=st.columns(2)
            f1.download_button("⬇️ Resumen CSV",to_csv(sm3),f"{base}_clusters.csv","text/csv",use_container_width=True)
            f2.download_button("⬇️ Resumen XLSX",to_xlsx(sm3,"Clusters"),f"{base}_clusters.xlsx","application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",use_container_width=True)
        gc2=ss.get("geo_cache",{})
        if gc2:
            st.divider(); st.subheader("🗄️ Caché geocodificación")
            st.caption(f"{len(gc2):,} dirs. Descárgala para reutilizar sin gastar cuota.")
            cd=pd.DataFrame([{"dir_normalizada":a,"latitud":lt,"longitud":ln} for a,(lt,ln) in gc2.items()])
            st.download_button("⬇️ Caché CSV",to_csv(cd),"geo_cache.csv","text/csv",use_container_width=True)
        # ── Exportar mapa desde sesión activa ────────────────────────────────
        df_for_map=_first_df("df_clustered","df_geocoded")
        if df_for_map is not None and _FOLIUM:
            lc_ex=lat_col(df_for_map); oc_ex=lon_col(df_for_map)
            if lc_ex and oc_ex:
                st.divider(); st.subheader("🗺️ Exportar mapa HTML")
                st.caption("Genera el mapa actual y lo descarga como archivo `.html` interactivo. "
                           "Se puede abrir en cualquier navegador sin instalar nada.")
                mc_ex1,mc_ex2=st.columns([2,1])
                mode_ex=mc_ex1.selectbox("Tipo de mapa a exportar",
                    ["Puntos simples","Heatmap (densidad)","Clusters coloreados","Círculos proporcionales"],
                    key="exp_mode")
                tile_ex=mc_ex2.selectbox("Mapa base",list(TILES.keys()),key="exp_tile")
                nm_ex=df_for_map.select_dtypes(include="number").columns.tolist()
                kgex=kg_col(df_for_map); pedex=ped_col(df_for_map)
                scol_ex=None
                if mode_ex in ("Círculos proporcionales","Clusters coloreados"):
                    di_ex=nm_ex.index(kgex) if kgex in nm_ex else 0
                    scol_ex=st.selectbox("Variable de tamaño",nm_ex,index=di_ex,key="exp_scol") if nm_ex else None
                tips_ex=[c for c in df_for_map.columns if c.lower() in ("cliente_id","kg","num_pedidos","pedidos")][:3]
                vm_ex=(pd.to_numeric(df_for_map[lc_ex],errors="coerce").notna() &
                       pd.to_numeric(df_for_map[oc_ex],errors="coerce").notna())
                df_map_ex=df_for_map[vm_ex]
                maxp_ex=st.slider("Máx. puntos en mapa exportado",1000,min(100_000,len(df_map_ex)),
                                   min(20_000,len(df_map_ex)),1000,key="exp_maxp")
                if len(df_map_ex)>maxp_ex: df_map_ex=df_map_ex.sample(n=maxp_ex,random_state=42)
                if st.button("🗺️ Generar y descargar mapa HTML",use_container_width=True,type="primary",key="exp_map_btn"):
                    with st.spinner("Generando mapa…"):
                        try:
                            hcl_ex="cluster" in df_map_ex.columns
                            if   mode_ex=="Puntos simples":       mo_ex=mapa_puntos(df_map_ex,lc_ex,oc_ex,tips_ex,tile_ex,"#00c9a7")
                            elif mode_ex=="Heatmap (densidad)":   mo_ex=mapa_heat(df_map_ex,lc_ex,oc_ex,kgex,tile_ex)
                            elif mode_ex=="Clusters coloreados":  mo_ex=mapa_clusters(df_map_ex,lc_ex,oc_ex,"cluster" if hcl_ex else lc_ex,tips_ex,tile_ex,scol_ex)
                            else:
                                if scol_ex: mo_ex=mapa_prop(df_map_ex,lc_ex,oc_ex,scol_ex,tips_ex,tile_ex,"#ffd166")
                                else: mo_ex=mapa_puntos(df_map_ex,lc_ex,oc_ex,tips_ex,tile_ex,"#00c9a7")
                            html_b=mapa_a_html(mo_ex)
                            fname_ex=f"mapa_{base}_{mode_ex.split()[0].lower()}.html"
                            st.download_button("⬇️ Descargar mapa HTML",html_b,fname_ex,"text/html",use_container_width=True)
                            st.success(f"✅ Mapa generado con {len(df_map_ex):,} puntos.")
                        except Exception as e:
                            st.error(f"❌ Error: {e}")

        # ── Estimador de costos API ───────────────────────────────────────────
        st.divider(); st.subheader("💰 Estimador de costos Google Maps API")
        n_est=st.number_input("¿Cuántas direcciones únicas necesitas geocodificar?",
                               min_value=0, max_value=1_000_000, value=1000, step=500, key="cost_est")
        cached_est=len(ss.get("geo_cache",{}))
        pend_est=max(0, n_est - cached_est)
        credit_free=200.0; price_per_1k=5.0
        cost_total=pend_est/1000*price_per_1k; cost_after_free=max(0.0, cost_total-credit_free)
        ec1,ec2,ec3,ec4=st.columns(4)
        ec1.metric("Dirs. a geocodificar",f"{pend_est:,}",f"{n_est-pend_est:,} en caché")
        ec2.metric("Costo bruto estimado",f"${cost_total:.2f} USD")
        ec3.metric("Crédito gratuito Google","$200 USD/mes")
        ec4.metric("Costo neto estimado",f"${cost_after_free:.2f} USD",
                   "¡GRATIS!" if cost_after_free==0 else None)
        if cost_after_free == 0:
            st.success("✅ Dentro del crédito gratuito mensual de Google ($200 USD). **Costo neto: $0**.")
        else:
            st.warning(f"⚠️ Este volumen supera el crédito gratuito. Costo adicional estimado: **${cost_after_free:.2f} USD**.")
        st.caption("*Tarifa de referencia: ~$5 USD por 1.000 geocodificaciones. "
                   "Google ofrece $200 USD/mes de crédito gratuito (~40.000 geocodificaciones gratis). "
                   "Precios sujetos a cambio. Verifica en [Google Maps Pricing](https://mapsplatform.google.com/pricing/).*")

        with st.expander("👁️ Vista previa"): st.dataframe(df_ex.head(100),use_container_width=True,height=380)
