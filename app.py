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
        "n_clusters":8,"file_name":"","stop_geocoding":False,
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
# NORMALIZACIÓN
# ══════════════════════════════════════════════════════════════════════════════
ABBR = {
    r'\b(?:Cra|Cr|kr|KR|akr|AKR)\b\.?':"Carrera", r'\b(?:Cll|Cl|CL|cll|ac|AC)\b\.?':"Calle",
    r'\b(?:Av|Avda|Avd|AV)\b\.?':"Avenida", r'\b(?:Diag|Dg|DG|diag)\b\.?':"Diagonal",
    r'\b(?:Transv|Trans|Trv|Tv|TV|transv)\b\.?':"Transversal",
    r'\bAut(?:p|op)?\b\.?':"Autopista", r'\bCirc(?:\.?|unv\.?)\b':"Circunvalar",
    r'\bNo\.?\b|N°':"#", r'\bKm\.?\b|\bK\.?\b':"Kilometro", r'\bInt\.?\b':"Interior",
    r'\bEd(?:if)?\.?\b':"Edificio", r'\bApt(?:o)?\.?\b':"Apartamento", r'\bEsq\.?\b':"Esquina",
    r'\bMz(?:n)?\.?\b':"Manzana", r'\bLt\.?\b':"Lote", r'\bUrb\.?\b':"Urbanizacion",
    r'\bBrr\.?\b':"Barrio", r'\bPq\.?\b':"Parque", r'\bPt?e\.?\b':"Puente",
}

def norm_addr(addr, country="Colombia"):
    if not isinstance(addr,str) or not addr.strip(): return ""
    r = addr.strip()
    for p,rep in ABBR.items(): r = re.sub(p,rep,r,flags=re.IGNORECASE)
    r = re.sub(r'\s+',' ',r).strip()
    if country and country.lower() not in r.lower(): r = f"{r}, {country}"
    return r

def norm_col(df, addr_col, country="Colombia", out="dir_normalizada"):
    df = df.copy()
    df[out] = df[addr_col].astype(str).apply(lambda a: norm_addr(a,country))
    return df

def addr_stats(df, col):
    t=len(df); e=int(df[col].isna().sum()+(df[col].astype(str).str.strip()=="").sum())
    n=int(df[col].astype(str).str.contains(r'\d',na=False).sum())
    return {"total":t,"empty":e,"with_num":n,"pct_ok":round(n/max(t-e,1)*100,1)}

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
    "sin_peso":"Sin peso — todos iguales","kg":"⚖️ Por KG — volumen",
    "pedidos":"📦 Por Pedidos — frecuencia","combinado":"⚡ Combinado 60%KG+40%Ped",
}

def build_w(df, wt, kgc, pedc):
    if wt=="sin_peso": return None
    def safe(c): return pd.to_numeric(df[c],errors="coerce").fillna(0).clip(lower=0).values \
                        if c and c in df.columns else np.zeros(len(df))
    def norm(a): mx=a.max(); return a/mx if mx>0 else a
    if wt=="kg":        w=safe(kgc)
    elif wt=="pedidos": w=safe(pedc)
    else:               w=norm(safe(kgc))*0.6+norm(safe(pedc))*0.4
    return np.clip(w,0,None)

def do_cluster(df, lc, oc, method, params, wt, kgc, pedc):
    df=df.copy()
    ln=pd.to_numeric(df[lc],errors="coerce"); lo=pd.to_numeric(df[oc],errors="coerce")
    v=ln.notna()&lo.notna()&ln.between(-90,90)&lo.between(-180,180)
    dv,dx=df[v].copy(),df[~v].copy()
    if len(dv)<3: return df,{"error":f"Solo {len(dv)} puntos válidos."}
    X=np.column_stack([ln[v].values,lo[v].values]); W=build_w(dv,wt,kgc,pedc)
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
@import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Sans:wght@300;400;600;700&display=swap');
html,body,[class*="css"]{font-family:'IBM Plex Sans',sans-serif;}
.gh{background:linear-gradient(135deg,#0d2137,#1a1f35);border-left:5px solid #00c9a7;
    border-radius:10px;padding:1.2rem 1.8rem;margin-bottom:1rem;}
.gh h1{color:#00c9a7;font-size:1.7rem;font-weight:700;margin:0;}
.gh p{color:#a8b4c8;margin:.2rem 0 0;font-size:.88rem;}
.stTabs [data-baseweb="tab-list"]{gap:4px;}
.stTabs [data-baseweb="tab"]{background:#1a1f2e;border-radius:8px 8px 0 0;font-weight:600;font-size:.82rem;}
.stTabs [aria-selected="true"]{background:#00c9a7 !important;color:#0f1117 !important;}
section[data-testid="stSidebar"]{background:#0f1117;border-right:1px solid #1a1f2e;}
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
            hc=bool(ltc and lnc)
            c1,c2,c3,c4=st.columns(4)
            c1.metric("Filas",f"{len(df_l):,}"); c2.metric("Columnas",len(df_l.columns))
            c3.metric("Nulos",f"{int(df_l.isnull().sum().sum()):,}"); c4.metric("¿Coords?","✅" if hc else "❌")
            if hc: st.success("✅ Coordenadas detectadas — puedes saltar geocodificación.")
            st.dataframe(df_l.head(50),use_container_width=True,height=250)
            st.divider()
            if st.button("✅ Confirmar carga →",use_container_width=True,type="primary"):
                st.session_state.update({"df_original":df_l.copy(),"df_working":df_l.copy(),
                    "step_uploaded":True,"file_name":up.name,"col_mapping":auto_map(df_l)})
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
        df_ft=ss.get("df_working") or ss.get("df_original")
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
                if   pr=="Últimos 30 días":  df,dt=td-timedelta(30),td
                elif pr=="Últimos 3 meses":  df2,dt=td-timedelta(90),td
                elif pr=="Últimos 6 meses":  df2,dt=td-timedelta(180),td
                elif pr=="Último año":        df2,dt=td-timedelta(365),td
                elif pr=="Rango personalizado":
                    fc1,fc2=st.columns(2); df2=fc1.date_input("Desde",md,min_value=md,max_value=mxd); dt=fc2.date_input("Hasta",mxd,min_value=md,max_value=mxd)
                else: df2,dt=md,mxd
                if pr=="Últimos 30 días": df2=td-timedelta(30)
                mask=df_ft2[fc].dt.date.between(df2,dt); flt=df_ft2[mask]
                st.metric("Filas en período",f"{len(flt):,}"); st.divider()
                if st.button("✅ Aplicar filtro →",use_container_width=True,type="primary"):
                    st.session_state.update({"df_filtered":flt.copy(),"step_filtered":True})
                    invalidate_from("normalize"); st.success(f"✅ {len(flt):,} filas del {df2} al {dt}.")

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
                st.divider(); st.subheader("⚙️ Configurar")
                akey=ss.get("google_api_key","")
                if not akey: st.warning("⚠️ Ingresa tu API Key en el panel lateral.")
                else:
                    ac2=[c for c in df_gc.columns if c in ("dir_normalizada","direccion","address")]
                    if not ac2: ac2=df_gc.select_dtypes(include="object").columns.tolist()
                    g1,g2=st.columns(2)
                    acg=g1.selectbox("Columna dirección",ac2 or df_gc.columns.tolist()); dlg=g2.slider("Delay (s)",0.02,2.0,0.05,0.01)
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
                            try: df_g2,gs=geo_df(df_gc,acg,akey,dlg,_p)
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
            elif ss.get("step_geocoded") and ss.get("df_geocoded") is not None:
                dg3=ss["df_geocoded"]; n2=int(dg3["latitud"].notna().sum()) if "latitud" in dg3.columns else 0
                st.success(f"✅ Geocodificado: **{n2:,}** registros con coordenadas.")

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
                    st.subheader("③ Variable de gravedad")
                    st.info("Pondera cada punto según su volumen o frecuencia al formar clusters.")
                    wt=st.radio("Gravedad",list(WEIGHTS.keys()),format_func=lambda k:WEIGHTS[k],horizontal=True)
                    kgs=peds=None
                    if wt in ("kg","combinado"):
                        di=nc.index(kgd) if kgd in nc else 0; kgs=st.selectbox("Columna KG",nc,index=di,key="sk")
                    if wt in ("pedidos","combinado"):
                        di=nc.index(pedd) if pedd in nc else 0; peds=st.selectbox("Columna Pedidos",nc,index=di,key="sp")
                    st.divider()
                    if st.button("🚀 Clusterizar",use_container_width=True,type="primary"):
                        with st.spinner(f"Ejecutando {inf2.label}…"):
                            dcl,cst=do_cluster(df_cl,lc3,oc3,meth,pars,wt,kgs,peds)
                        if "error" in cst: st.error(f"❌ {cst['error']}")
                        else:
                            st.session_state.update({"df_clustered":dcl,"step_clustered":True})
                            k1,k2,k3,k4=st.columns(4)
                            k1.metric("Clusters",cst["n_clusters"]); k2.metric("Puntos",f"{cst['n_points']:,}")
                            k3.metric("Ruido",f"{cst['noise']:,}"); k4.metric("% sin cluster",f"{cst['pct_noise']}%")
                            st.dataframe(cst["summary"],use_container_width=True)
                            st.success("✅ Ve a **🗺️ Mapas** o **💾 Exportar**.")

# ── TAB 7: MAPAS ──────────────────────────────────────────────────────────────
with T[6]:
    st.header("🗺️ Visualización en Mapas")
    if not _FOLIUM: st.error("❌ Instala `folium` y `streamlit-folium`.")
    elif not ss.get("step_mapped"): st.warning("⚠️ Completa el Mapeo.")
    else:
        df_mp=(ss.get("df_clustered") or ss.get("df_geocoded") or ss.get("df_normalized") or ss.get("df_working"))
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
                            elif mode=="Clusters coloreados":                   mo=mapa_clusters(df_m,lc4,oc4,ccm or "cluster",tips,tile,scm)
                            elif mode=="Agrupación automática (MarkerCluster)": mo=mapa_mc(df_m,lc4,oc4,tips,tile)
                            elif mode=="Círculos proporcionales":
                                if not scm: st.warning("Selecciona variable de tamaño."); mo=None
                                else: mo=mapa_prop(df_m,lc4,oc4,scm,tips,tile,colm)
                            if mo: st_folium(mo,width="100%",height=540,returned_objects=[])
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
        with st.expander("👁️ Vista previa"): st.dataframe(df_ex.head(100),use_container_width=True,height=380)
