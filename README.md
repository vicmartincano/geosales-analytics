# 🗺️ GeoSales Analytics

Aplicación Streamlit para análisis geoespacial de ventas y pedidos.  
Normaliza direcciones colombianas, geocodifica con Google Maps API, clusteriza puntos y genera mapas interactivos.

[![Open in Streamlit](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://tu-app.streamlit.app)

---

## Pipeline

```
📂 Carga → 🔗 Mapeo → 📅 Filtro → ✏️ Normalizar → 📍 Geocodificar → 🔵 Clusterizar → 🗺️ Mapas → 💾 Exportar
```

> **Atajo:** Si ya tienes coordenadas en tu archivo → Carga → Mapeo → Clusterizar / Mapas directo.

---

## 🚀 Desplegar en Streamlit Community Cloud (gratis)

1. **Haz fork** de este repositorio en tu cuenta de GitHub
2. Ve a [share.streamlit.io](https://share.streamlit.io/) e inicia sesión con GitHub
3. Haz clic en **"New app"**
4. Selecciona tu repositorio, rama `main` y archivo `app.py`
5. Haz clic en **"Deploy"** — listo en ~2 minutos

> La API key de Google Maps se ingresa directamente en la app (panel lateral) por sesión.  
> **Nunca** la escribas en el código ni en archivos del repositorio.

---

## 💻 Correr localmente

```bash
# 1. Clonar
git clone https://github.com/TU_USUARIO/geosales-analytics.git
cd geosales-analytics

# 2. Entorno virtual
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate

# 3. Instalar dependencias
pip install -r requirements.txt

# 4. Ejecutar
streamlit run app.py
```

La app abre en `http://localhost:8501`

---

## 📁 Estructura del repositorio

```
geosales-analytics/
│
├── app.py                        ← Entrada principal (Streamlit lee este archivo)
├── requirements.txt              ← Dependencias (Streamlit Cloud las instala automáticamente)
├── README.md
├── .gitignore
│
├── .streamlit/
│   └── config.toml               ← Tema oscuro y configuración del servidor
│
├── components/                   ← Una función render_*() por pestaña
│   ├── sidebar.py
│   ├── tab_upload.py
│   ├── tab_mapping.py
│   ├── tab_filter.py
│   ├── tab_normalize.py
│   ├── tab_geocode.py
│   ├── tab_cluster.py
│   ├── tab_maps.py
│   └── tab_export.py
│
├── services/                     ← Lógica pura, sin UI
│   ├── normalizer.py             ← Expansión abreviaturas colombianas
│   ├── geocoder.py               ← Google Maps Geocoding API
│   ├── clusterer.py              ← KMeans / MiniBatchKMeans / DBSCAN
│   └── mapper.py                 ← Generación de mapas Folium
│
├── utils/
│   └── session.py                ← Estado global st.session_state
│
└── sample_data/
    └── sample_ventas_bogota.csv  ← 500 filas de prueba (sin datos reales)
```

---

## 📊 Formato del archivo de entrada

| Columna canónica | Descripción                        | Requerido |
|------------------|------------------------------------|-----------|
| `cliente_id`     | ID del cliente                     | ✅        |
| `direccion`      | Dirección de entrega               | ✅ *      |
| `kg`             | Kilogramos                         | Recomendado |
| `num_pedidos`    | Número de pedidos                  | Recomendado |
| `latitud`        | Latitud (si ya está geocodificado) | ✅ *      |
| `longitud`       | Longitud (si ya está geocodificado)| ✅ *      |
| `comprador`      | Nombre del comprador               | Opcional  |
| `fecha`          | Fecha del pedido                   | Opcional  |

*Se requiere `direccion` **o** `latitud + longitud`.

Los nombres de columna pueden ser distintos — la app los mapea automáticamente o te pide que los asignes.

---

## 🔑 Google Maps API Key

La geocodificación usa [Google Maps Geocoding API](https://developers.google.com/maps/documentation/geocoding).

**Cómo obtener tu clave:**
1. Ve a [console.cloud.google.com](https://console.cloud.google.com/)
2. Crea un proyecto y habilita **Geocoding API**
3. Ve a **Credenciales → Crear credencial → Clave de API**
4. (Opcional) Restringe la clave a tu IP o dominio

**Costo estimado:** ~$5 USD por cada 1 000 direcciones únicas geocodificadas.

**Seguridad:** La clave se ingresa en el panel lateral de la app y se guarda solo en memoria de sesión (`st.session_state`). Nunca se escribe en código, archivos ni logs.

---

## 🔵 Métodos de clusterización

| Método           | Cuándo usarlo                              | Necesita K |
|------------------|--------------------------------------------|------------|
| **K-Means**      | Zonas balanceadas, K conocido              | ✅ Sí      |
| **Mini-Batch KM**| Datasets grandes (>50 k puntos)           | ✅ Sí      |
| **DBSCAN**       | Clusters irregulares, detectar outliers    | ❌ No      |

## ⚖️ Opciones de gravedad (ponderación)

| Opción      | Efecto                                             |
|-------------|----------------------------------------------------|
| Sin peso    | Todos los puntos valen igual                       |
| Por KG      | Clientes con más volumen atraen más al centroide  |
| Por Pedidos | Clientes más frecuentes tienen más influencia      |
| Combinado   | 60 % KG + 40 % Pedidos                            |

## 🗺️ Modos de visualización

- **Puntos simples** — Vista básica con tooltips personalizables
- **Heatmap** — Densidad de puntos, con peso opcional (kg, pedidos)
- **Clusters coloreados** — Cada cluster en un color distinto, tamaño proporcional opcional
- **MarkerCluster** — Agrupación automática de Leaflet (zoom para explorar)
- **Círculos proporcionales** — Tamaño del círculo ∝ variable elegida

---

## ⚙️ Dependencias

```
streamlit>=1.35.0
pandas>=2.0.0
numpy>=1.24.0
openpyxl>=3.1.0
requests>=2.31.0
scikit-learn>=1.4.0
folium>=0.17.0
streamlit-folium>=0.20.0
xlsxwriter>=3.1.0
```

---

## 🔒 Seguridad y buenas prácticas

- ✅ API key solo en sesión (`st.session_state`), nunca en código
- ✅ `.gitignore` excluye `.env`, CSV con datos reales, cachés
- ✅ No hay credenciales hardcodeadas en ningún archivo
- ✅ Los datos del usuario solo viven en la sesión del navegador
- ⚠️ No subas archivos con datos reales de clientes al repositorio

---

## 📝 Licencia

MIT — úsalo, modifícalo, distribúyelo libremente.
