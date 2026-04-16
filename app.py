"""
Cruce de Inventario CEDIS ↔ Tiendas GPNA
Aplicación web con Streamlit + conexión a Odoo 17+
"""

import streamlit as st
import xmlrpc.client
import pandas as pd
from datetime import datetime, date, timedelta
from collections import defaultdict
import re
import io

# ============================================================
# CONFIGURACIÓN DE LA PÁGINA
# ============================================================
st.set_page_config(
    page_title="Cruce Inventario | Bio Zen ↔ GPNA",
    page_icon="📦",
    layout="wide",
)

# ============================================================
# ESTILOS
# ============================================================
st.markdown("""
<style>
    .main-header {
        display: flex; align-items: center; gap: 16px;
        padding: 10px 0 20px 0;
    }
    .logo-box {
        width: 50px; height: 50px; border-radius: 14px;
        background: linear-gradient(135deg, #1a5c3a, #2a8f5a);
        display: flex; align-items: center; justify-content: center;
        color: white; font-weight: 800; font-size: 20px;
    }
    .metric-ok { color: #10b981; }
    .metric-faltante { color: #ef4444; }
    .metric-sobrante { color: #f59e0b; }
    .stTabs [data-baseweb="tab-list"] { gap: 8px; }
    .stTabs [data-baseweb="tab"] {
        padding: 10px 20px; border-radius: 8px 8px 0 0;
    }
</style>
""", unsafe_allow_html=True)

# ============================================================
# CONFIGURACIÓN DE ODOO (en secrets para producción)
# ============================================================
ODOO_URL = st.secrets.get("ODOO_URL", "https://grupo-biozen130426.odoo.com")
ODOO_DB = st.secrets.get("ODOO_DB", "grupo-biozen130426")
ODOO_USER = st.secrets.get("ODOO_USER", "procesos@grupobiozen.com")
ODOO_PASS = st.secrets.get("ODOO_PASS", "73670480d505734b15b41a73e326221071fd8074")

COMPANY_CEDIS_ID = 1
COMPANY_TIENDAS_ID = 3

GPNA_PARTNER_IDS = [
    18196, 18199, 18205, 18207, 18209, 18211, 18213, 18215,
    18217, 18219, 18222, 18221, 18220, 18218, 18216, 18210,
    18208, 18212, 18206, 18204, 18203, 18202, 18201, 18200,
    18198, 18194, 18116, 19184, 18197, 18195,
]

SUCURSALES_NOMBRES = {
    18196: "CENTRO", 18199: "CONCHI", 18205: "CARRASCO",
    18207: "SERDAN", 18209: "ROSARIO", 18211: "ESCUINAPA",
    18213: "INSURGENTES", 18215: "VILLA UNIÓN", 18217: "SANTA ROSA",
    18219: "LEY VIEJA", 18222: "CARIBE", 18221: "LEY DEL MAR",
    18220: "BRAVO", 18218: "AQUILES", 18216: "RELIGIOSO",
    18210: "VILLA VERDE", 18208: "ESCOBEDO", 18212: "FORJADORES",
    18206: "GUAYMITAS", 18204: "MERCADITO", 18203: "LOS MANGOS",
    18202: "COLA DE BALLENA", 18201: "SAN JOSÉ VIEJO", 18200: "TOREO",
    18198: "HIDALGO", 18194: "COLOSIO", 18116: "LA CAMPIÑA",
    19184: "SUPER GPNA", 18197: "MELCHOR OCAMPO", 18195: "DELIVERY",
}


# ============================================================
# FUNCIONES DE CONEXIÓN Y CONSULTA
# ============================================================
def conectar_odoo():
    common = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/common")
    uid = common.authenticate(ODOO_DB, ODOO_USER, ODOO_PASS, {})
    if not uid:
        return None, None
    models = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
    return uid, models


def query(models, uid, modelo, dominio, campos, extra=None):
    params = {'fields': campos}
    if extra:
        params.update(extra)
    try:
        return models.execute_kw(ODOO_DB, uid, ODOO_PASS, modelo, 'search_read', [dominio], params)
    except Exception:
        # Reconectar si la conexión se perdió
        models_new = xmlrpc.client.ServerProxy(f"{ODOO_URL}/xmlrpc/2/object")
        return models_new.execute_kw(ODOO_DB, uid, ODOO_PASS, modelo, 'search_read', [dominio], params)


def extraer_folio_s(purchase_name):
    match = re.search(r'\((S\d+)\)', str(purchase_name))
    return match.group(1) if match else None


# ============================================================
# FUNCIONES DE NEGOCIO
# ============================================================
def obtener_surtidos_por_folios(models, uid, folios_s, partner_ids):
    """Trae TODAS las entregas (incluyendo parciales) de los folios S dados."""
    if not folios_s:
        return {}

    pickings = query(models, uid, 'stock.picking', [
        ('company_id', '=', COMPANY_CEDIS_ID),
        ('picking_type_code', '=', 'outgoing'),
        ('state', '=', 'done'),
        ('origin', 'in', list(folios_s)),
        ('partner_id', 'in', partner_ids),
    ], ['id', 'name', 'partner_id', 'origin', 'date_done', 'sale_id'])

    surtidos = {}
    for p in pickings:
        folio_s = p['origin'] or ''
        if not folio_s.startswith('S'):
            continue
        if folio_s not in surtidos:
            surtidos[folio_s] = {
                'sucursal': p['partner_id'][1].replace('TIENDAS GPNA, ', '') if p['partner_id'] else 'Sin sucursal',
                'pickings': [],
                'productos': defaultdict(lambda: {'nombre': '', 'cantidad': 0, 'uom': '', 'lotes': set()}),
                'fecha': p['date_done'],
            }
        surtidos[folio_s]['pickings'].append(p['name'])

        lineas = query(models, uid, 'stock.move.line', [
            ('picking_id', '=', p['id']),
            ('state', '=', 'done'),
        ], ['product_id', 'quantity', 'lot_id', 'product_uom_id'])

        for l in lineas:
            if not l['product_id']:
                continue
            pid = l['product_id'][0]
            surtidos[folio_s]['productos'][pid]['nombre'] = l['product_id'][1]
            surtidos[folio_s]['productos'][pid]['cantidad'] += l['quantity']
            surtidos[folio_s]['productos'][pid]['uom'] = l['product_uom_id'][1] if l['product_uom_id'] else ''
            if l['lot_id']:
                surtidos[folio_s]['productos'][pid]['lotes'].add(l['lot_id'][1])

    return surtidos


def obtener_recepciones_por_folios(models, uid, folios_s):
    """Trae recepciones vinculadas a los folios S, buscando en rango amplio."""
    if not folios_s:
        return {}

    # Buscar recepciones en un rango amplio (últimos 60 días)
    # y filtrar las que correspondan a nuestros folios S
    from datetime import datetime, timedelta
    fecha_limite = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")

    pickings_raw = query(models, uid, 'stock.picking', [
        ('company_id', '=', COMPANY_TIENDAS_ID),
        ('picking_type_code', '=', 'incoming'),
        ('state', '=', 'done'),
        ('date_done', '>=', f"{fecha_limite} 00:00:00"),
    ], ['id', 'name', 'partner_id', 'origin', 'date_done', 'purchase_id'])

    # Filtrar solo las que corresponden a nuestros folios S
    pickings = []
    for p in pickings_raw:
        purchase_name = p['purchase_id'][1] if p['purchase_id'] else ''
        folio_s = extraer_folio_s(purchase_name)
        if folio_s and folio_s in folios_s:
            pickings.append(p)

    recepciones = {}
    for p in pickings:
        purchase_name = p['purchase_id'][1] if p['purchase_id'] else ''
        folio_s = extraer_folio_s(purchase_name)
        if not folio_s:
            continue

        sucursal_picking = p['name'].split('/')[0] if '/' in p['name'] else ''
        if folio_s not in recepciones:
            recepciones[folio_s] = {
                'sucursal_picking': sucursal_picking,
                'pickings': [],
                'productos': defaultdict(lambda: {'nombre': '', 'cantidad': 0, 'uom': '', 'lotes': set()}),
                'fecha': p['date_done'],
                'folio_p': p['origin'] or '',
            }
        recepciones[folio_s]['pickings'].append(p['name'])

        lineas = query(models, uid, 'stock.move.line', [
            ('picking_id', '=', p['id']),
            ('state', '=', 'done'),
        ], ['product_id', 'quantity', 'lot_id', 'product_uom_id'])

        for l in lineas:
            if not l['product_id']:
                continue
            pid = l['product_id'][0]
            recepciones[folio_s]['productos'][pid]['nombre'] = l['product_id'][1]
            recepciones[folio_s]['productos'][pid]['cantidad'] += l['quantity']
            recepciones[folio_s]['productos'][pid]['uom'] = l['product_uom_id'][1] if l['product_uom_id'] else ''
            if l['lot_id']:
                recepciones[folio_s]['productos'][pid]['lotes'].add(l['lot_id'][1])

    return recepciones


def cruzar(surtidos, recepciones):
    todos_folios = set(surtidos.keys()) | set(recepciones.keys())
    resultados = []

    for folio_s in sorted(todos_folios):
        surt = surtidos.get(folio_s)
        recep = recepciones.get(folio_s)

        sucursal = surt['sucursal'] if surt else (recep['sucursal_picking'] if recep else 'N/A')
        folio_p = recep['folio_p'] if recep else '-'
        pickings_surt = ', '.join(set(surt['pickings'])) if surt else '-'
        pickings_recep = ', '.join(set(recep['pickings'])) if recep else '-'

        productos_surt = surt['productos'] if surt else {}
        productos_recep = recep['productos'] if recep else {}
        todos_productos = set(productos_surt.keys()) | set(productos_recep.keys())

        for pid in sorted(todos_productos):
            ps = productos_surt.get(pid, {'nombre': 'N/A', 'cantidad': 0, 'uom': '', 'lotes': set()})
            pr = productos_recep.get(pid, {'nombre': 'N/A', 'cantidad': 0, 'uom': '', 'lotes': set()})

            cant_s = ps['cantidad']
            cant_r = pr['cantidad']
            dif = cant_s - cant_r

            if dif > 0:
                estado = "FALTANTE"
            elif dif < 0:
                estado = "SOBRANTE"
            else:
                estado = "OK"

            nombre_prod = ps['nombre'] if ps['nombre'] != 'N/A' else pr['nombre']
            uom = ps['uom'] or pr['uom']

            resultados.append({
                'Folio Venta': folio_s,
                'Folio Compra': folio_p,
                'Sucursal': sucursal,
                'Producto': nombre_prod,
                'UdM': uom,
                'Surtido': cant_s,
                'Recibido': cant_r,
                'Diferencia': abs(dif),
                'Estado': estado,
                'Docs Surtido': pickings_surt,
                'Docs Recepción': pickings_recep,
            })

    return pd.DataFrame(resultados)


# ============================================================
# INTERFAZ
# ============================================================

# Header
st.markdown("""
<div class="main-header">
    <div class="logo-box">BZ</div>
    <div>
        <h1 style="margin:0; font-size:24px; color:#0f1a2a;">Cruce de Inventario</h1>
        <p style="margin:0; font-size:14px; color:#8896a4;">CEDIS Bio Zen → Tiendas GPNA</p>
    </div>
</div>
""", unsafe_allow_html=True)

# Sidebar con filtros
with st.sidebar:
    st.markdown("### 📋 Configuración del Reporte")
    st.markdown("---")

    st.markdown("**📅 Rango de Fechas**")
    col1, col2 = st.columns(2)
    with col1:
        fecha_inicio = st.date_input("Desde", value=date.today() - timedelta(days=7))
    with col2:
        fecha_fin = st.date_input("Hasta", value=date.today())

    st.markdown("---")
    st.markdown("**🏪 Sucursales**")

    todas = st.checkbox("Todas las sucursales", value=True)
    if todas:
        sucursales_sel = list(SUCURSALES_NOMBRES.keys())
    else:
        opciones = {v: k for k, v in SUCURSALES_NOMBRES.items()}
        seleccion = st.multiselect(
            "Selecciona sucursales:",
            options=sorted(opciones.keys()),
            default=[]
        )
        sucursales_sel = [opciones[s] for s in seleccion]

    st.markdown("---")
    ejecutar = st.button("🚀 Ejecutar Cruce", type="primary", use_container_width=True)

# Conexión y ejecución
if ejecutar:
    if not sucursales_sel:
        st.error("Selecciona al menos una sucursal.")
    elif fecha_inicio > fecha_fin:
        st.error("La fecha de inicio no puede ser mayor a la fecha fin.")
    else:
        with st.spinner("Conectando a Odoo..."):
            uid, models = conectar_odoo()

        if not uid:
            st.error("No se pudo conectar a Odoo. Verifica las credenciales.")
        else:
            st.success(f"Conectado a Odoo (UID: {uid})")

            progress = st.progress(0, text="Descubriendo folios en el rango de fechas...")

            # PASO 1: Descubrir folios S desde AMBOS lados
            # Desde surtidos del CEDIS
            pickings_cedis = query(models, uid, 'stock.picking', [
                ('company_id', '=', COMPANY_CEDIS_ID),
                ('picking_type_code', '=', 'outgoing'),
                ('state', '=', 'done'),
                ('scheduled_date', '>=', f"{fecha_inicio} 00:00:00"),
                ('scheduled_date', '<=', f"{fecha_fin} 23:59:59"),
                ('partner_id', 'in', sucursales_sel),
            ], ['origin'])

            folios_desde_cedis = set()
            for p in pickings_cedis:
                folio = p['origin'] or ''
                if folio.startswith('S'):
                    folios_desde_cedis.add(folio)

            # Desde recepciones en tiendas
            pickings_tiendas = query(models, uid, 'stock.picking', [
                ('company_id', '=', COMPANY_TIENDAS_ID),
                ('picking_type_code', '=', 'incoming'),
                ('state', '=', 'done'),
                ('scheduled_date', '>=', f"{fecha_inicio} 00:00:00"),
                ('scheduled_date', '<=', f"{fecha_fin} 23:59:59"),
            ], ['purchase_id'])

            folios_desde_tiendas = set()
            for p in pickings_tiendas:
                purchase_name = p['purchase_id'][1] if p['purchase_id'] else ''
                folio_s = extraer_folio_s(purchase_name)
                if folio_s:
                    folios_desde_tiendas.add(folio_s)

            # Unir folios de ambos lados
            todos_folios = folios_desde_cedis | folios_desde_tiendas
            progress.progress(20, text=f"Folios encontrados: {len(todos_folios)} (CEDIS: {len(folios_desde_cedis)}, Tiendas: {len(folios_desde_tiendas)})")

            if not todos_folios:
                st.warning("No se encontraron folios en el rango de fechas seleccionado.")
            else:
                # PASO 2: Traer TODOS los surtidos de esos folios (sin filtro de fecha)
                progress.progress(30, text="Consultando surtidos completos del CEDIS...")
                surtidos = obtener_surtidos_por_folios(models, uid, todos_folios, sucursales_sel)
                progress.progress(50, text=f"Surtidos: {len(surtidos)} folios")

                # PASO 3: Traer TODAS las recepciones de esos folios (sin filtro de fecha)
                progress.progress(60, text="Consultando recepciones completas en tiendas...")
                recepciones = obtener_recepciones_por_folios(models, uid, todos_folios)
                progress.progress(80, text=f"Recepciones: {len(recepciones)} folios")

                progress.progress(90, text="Cruzando datos...")
                df = cruzar(surtidos, recepciones)
                progress.progress(100, text="¡Listo!")

                st.session_state['df'] = df
                st.session_state['fecha_inicio'] = str(fecha_inicio)
                st.session_state['fecha_fin'] = str(fecha_fin)

# Mostrar resultados si existen
if 'df' in st.session_state:
    df = st.session_state['df']
    fi = st.session_state['fecha_inicio']
    ff = st.session_state['fecha_fin']

    total = len(df)
    ok = len(df[df['Estado'] == 'OK'])
    faltantes = len(df[df['Estado'] == 'FALTANTE'])
    sobrantes = len(df[df['Estado'] == 'SOBRANTE'])
    precision = (ok / total * 100) if total > 0 else 0

    # Métricas principales
    st.markdown(f"##### Período: {fi} a {ff}")
    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Productos", f"{total:,}")
    c2.metric("Coinciden ✓", f"{ok:,}")
    c3.metric("Faltantes ▼", f"{faltantes:,}")
    c4.metric("Sobrantes ▲", f"{sobrantes:,}")
    c5.metric("Precisión", f"{precision:.1f}%")

    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Resumen", "❌ Incongruencias", "📄 Detalle Completo", "📥 Descargar"])

    # TAB 1: Resumen por sucursal
    with tab1:
        st.markdown("#### Estado por Sucursal")
        resumen_suc = df.groupby('Sucursal')['Estado'].value_counts().unstack(fill_value=0)
        for col_name in ['OK', 'FALTANTE', 'SOBRANTE']:
            if col_name not in resumen_suc.columns:
                resumen_suc[col_name] = 0
        resumen_suc['Total'] = resumen_suc.sum(axis=1)
        resumen_suc = resumen_suc.sort_values('Total', ascending=False)
        resumen_suc = resumen_suc[['Total', 'OK', 'FALTANTE', 'SOBRANTE']]
        resumen_suc.columns = ['Total', '✓ OK', '▼ Faltantes', '▲ Sobrantes']

        st.dataframe(
            resumen_suc.style.map(
                lambda v: 'color: #ef4444; font-weight: bold' if v > 0 else '',
                subset=['▼ Faltantes']
            ).map(
                lambda v: 'color: #f59e0b; font-weight: bold' if v > 0 else '',
                subset=['▲ Sobrantes']
            ),
            use_container_width=True,
            height=400,
        )

        # Gráfica
        st.markdown("#### Distribución de Incongruencias")
        incong_suc = df[df['Estado'] != 'OK'].groupby('Sucursal').size().sort_values(ascending=True)
        if len(incong_suc) > 0:
            st.bar_chart(incong_suc)
        else:
            st.success("¡Sin incongruencias! Todo cuadra perfectamente.")

    # TAB 2: Solo incongruencias
    with tab2:
        st.markdown("#### Incongruencias Detectadas")
        df_incong = df[df['Estado'] != 'OK'].copy()

        if len(df_incong) > 0:
            col_f1, col_f2, col_f3 = st.columns(3)
            with col_f1:
                suc_filter = st.selectbox("Sucursal", ["Todas"] + sorted(df_incong['Sucursal'].unique().tolist()))
            with col_f2:
                est_filter = st.selectbox("Tipo", ["Todos", "FALTANTE", "SOBRANTE"])
            with col_f3:
                buscar = st.text_input("Buscar producto o folio")

            filtered = df_incong.copy()
            if suc_filter != "Todas":
                filtered = filtered[filtered['Sucursal'] == suc_filter]
            if est_filter != "Todos":
                filtered = filtered[filtered['Estado'] == est_filter]
            if buscar:
                mask = filtered['Producto'].str.contains(buscar, case=False, na=False) | filtered['Folio Venta'].str.contains(buscar, case=False, na=False)
                filtered = filtered[mask]

            st.markdown(f"**{len(filtered)} incongruencias encontradas**")

            st.dataframe(
                filtered.style.map(
                    lambda v: 'background-color: #fef2f2; color: #991b1b; font-weight: bold' if v == 'FALTANTE' else ('background-color: #fffbeb; color: #92400e; font-weight: bold' if v == 'SOBRANTE' else ''),
                    subset=['Estado']
                ),
                use_container_width=True,
                height=500,
                column_config={
                    "Surtido": st.column_config.NumberColumn(format="%.0f"),
                    "Recibido": st.column_config.NumberColumn(format="%.0f"),
                    "Diferencia": st.column_config.NumberColumn(format="%.0f"),
                },
            )
        else:
            st.success("🎉 ¡Sin incongruencias! Todo lo surtido coincide con lo recibido.")

    # TAB 3: Detalle completo
    with tab3:
        st.markdown("#### Todos los Registros")
        buscar_all = st.text_input("Buscar en todo", key="buscar_all")
        df_show = df.copy()
        if buscar_all:
            mask = df_show.apply(lambda row: buscar_all.lower() in str(row).lower(), axis=1)
            df_show = df_show[mask]

        st.dataframe(
            df_show.style.map(
                lambda v: 'background-color: #ecfdf5' if v == 'OK' else ('background-color: #fef2f2' if v == 'FALTANTE' else ('background-color: #fffbeb' if v == 'SOBRANTE' else '')),
                subset=['Estado']
            ),
            use_container_width=True,
            height=600,
            column_config={
                "Surtido": st.column_config.NumberColumn(format="%.0f"),
                "Recibido": st.column_config.NumberColumn(format="%.0f"),
                "Diferencia": st.column_config.NumberColumn(format="%.0f"),
            },
        )

    # TAB 4: Descargar
    with tab4:
        st.markdown("#### Descargar Reporte")

        col_d1, col_d2 = st.columns(2)

        with col_d1:
            st.markdown("**📊 Excel Completo**")
            buffer_xlsx = io.BytesIO()
            with pd.ExcelWriter(buffer_xlsx, engine='openpyxl') as writer:
                # Resumen
                resumen_export = df.groupby('Sucursal')['Estado'].value_counts().unstack(fill_value=0)
                for c in ['OK', 'FALTANTE', 'SOBRANTE']:
                    if c not in resumen_export.columns:
                        resumen_export[c] = 0
                resumen_export['Total'] = resumen_export.sum(axis=1)
                resumen_export.to_excel(writer, sheet_name='Resumen por Sucursal')

                # Detalle
                df.to_excel(writer, sheet_name='Detalle Completo', index=False)

                # Solo incongruencias
                df[df['Estado'] != 'OK'].to_excel(writer, sheet_name='Incongruencias', index=False)

            st.download_button(
                label="⬇️ Descargar Excel",
                data=buffer_xlsx.getvalue(),
                file_name=f"cruce_inventario_{fi}_a_{ff}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

        with col_d2:
            st.markdown("**📄 CSV**")
            csv_data = df.to_csv(index=False).encode('utf-8-sig')
            st.download_button(
                label="⬇️ Descargar CSV",
                data=csv_data,
                file_name=f"cruce_inventario_{fi}_a_{ff}.csv",
                mime="text/csv",
                use_container_width=True,
            )

        st.markdown("---")
        st.markdown("**📄 Solo Incongruencias (CSV)**")
        csv_incong = df[df['Estado'] != 'OK'].to_csv(index=False).encode('utf-8-sig')
        st.download_button(
            label="⬇️ Descargar solo incongruencias",
            data=csv_incong,
            file_name=f"incongruencias_{fi}_a_{ff}.csv",
            mime="text/csv",
        )

else:
    # Estado inicial
    st.info("👈 Selecciona las fechas y sucursales en el panel izquierdo, luego haz clic en **Ejecutar Cruce**.")
    st.markdown("""
    ### ¿Cómo funciona?
    1. **Selecciona las fechas** del período que quieres revisar
    2. **Elige las sucursales** (o deja "Todas" marcado)
    3. **Haz clic en "Ejecutar Cruce"**
    4. Revisa los resultados en las pestañas de Resumen, Incongruencias y Detalle
    5. **Descarga el Excel** con el reporte completo

    La herramienta compara automáticamente lo que el CEDIS surtió (folios S)
    contra lo que cada tienda GPNA recibió (folios P), producto por producto.
    """)
