import streamlit as st
import xmlrpc.client
import pandas as pd
import io
import datetime
from openpyxl import styles 

# --- CONFIGURACIÓN GLOBAL ---
st.set_page_config(page_title="Reportes Contables Odoo", layout="wide", page_icon="📊")

# ==========================================
# 1. BACKEND: CONEXIÓN Y PROCESAMIENTO
# ==========================================

def get_odoo_connection():
    try:
        url = st.secrets["odoo"]["url"]
        db = st.secrets["odoo"]["db"]
        username = st.secrets["odoo"]["username"]
        password = st.secrets["odoo"]["password"]
        
        common = xmlrpc.client.ServerProxy('{}/xmlrpc/2/common'.format(url))
        uid = common.authenticate(db, username, password, {})
        models = xmlrpc.client.ServerProxy('{}/xmlrpc/2/object'.format(url))
        return uid, models, db, password
    except Exception as e:
        st.error(f"Error de conexión: {e}")
        return None, None, None, None

def fetch_data(uid, models, db, password, cutoff_date):
    # 1. Traer líneas contables (SOLO COMPAÑÍA 1)
    # QUITAMOS 'x_studio_es_reintegro' de aquí porque suele estar en la cabecera, no en la línea.
    domain = [
        ('parent_state', '=', 'posted'),
        ('company_id', '=', 1),
        ('account_type', '=', 'liability_payable'),
        ('amount_residual', '!=', 0),
        ('move_id.move_type', 'in', ['in_invoice', 'in_refund']),
    ]
    
    fields = ['partner_id', 'date_maturity', 'date', 'ref', 
              'amount_residual', 'amount_residual_currency', 
              'currency_id', 'move_id'] # <--- Quitamos el campo de Studio de esta lista
    
    lines = models.execute_kw(db, uid, password, 'account.move.line', 'search_read', [domain], {'fields': fields})
    
    if not lines: return pd.DataFrame()

    df = pd.DataFrame(lines)
    
    # 2. CONSULTA ADICIONAL A LA CABECERA (account.move)
    # Aquí buscamos el campo de Reintegro y el Tipo de Documento
    move_ids_list = [m[0] for m in df['move_id'] if m]
    move_ids_unique = list(set(move_ids_list))
    
    type_map = {}
    reintegro_map = {}
    
    if move_ids_unique:
        # Pedimos los campos a la cabecera
        move_fields = ['move_type', 'x_studio_es_reintegro']
        moves_data = models.execute_kw(db, uid, password, 'account.move', 'search_read', 
                                       [[('id', 'in', move_ids_unique)]], 
                                       {'fields': move_fields})
        
        # Mapeamos los resultados
        for m in moves_data:
            type_map[m['id']] = m['move_type']
            # Ojo: x_studio_es_reintegro podría no venir si el campo no existe en algunas facturas viejas
            reintegro_map[m['id']] = m.get('x_studio_es_reintegro', False)

    # 3. Limpieza y Mapeo
    df['Proveedor'] = df['partner_id'].apply(lambda x: x[1] if x else 'Sin Proveedor')
    df['Partner_ID'] = df['partner_id'].apply(lambda x: x[0] if x else False)
    df['Moneda'] = df['currency_id'].apply(lambda x: x[1] if x else '')
    df['ref'] = df['ref'].apply(lambda x: x if x else '-')
    df['move_id_int'] = df['move_id'].apply(lambda x: x[0] if x else False)
    
    # APLICAMOS EL MAPEO DEL CAMPO STUDIO AL DATAFRAME
    df['x_studio_es_reintegro'] = df['move_id_int'].map(reintegro_map).fillna(False)
    
    # Corrección Fechas Vacías
    df['date_maturity'] = df.apply(lambda row: row['date'] if not row['date_maturity'] else row['date_maturity'], axis=1)
    df['date_maturity'] = pd.to_datetime(df['date_maturity'], errors='coerce')
    df = df.dropna(subset=['date_maturity'])

    # 4. Lógica de Montos (Moneda Original)
    def calcular_neto(row):
        tipo = type_map.get(row['move_id_int'], 'in_invoice')
        
        if row['amount_residual_currency'] and row['amount_residual_currency'] != 0:
            monto_base = row['amount_residual_currency']
        else:
            monto_base = row['amount_residual']
            
        monto_abs = abs(monto_base)
        
        if tipo == 'in_refund': 
            return -monto_abs
        return monto_abs

    df['amount_residual_neto'] = df.apply(calcular_neto, axis=1)

    # 5. Buckets
    fecha_corte_pd = pd.to_datetime(cutoff_date)
    df['dias_vencido'] = (fecha_corte_pd - df['date_maturity']).dt.days
    
    cols_bucket = ['En Fecha', '1-30', '31-60', '61-90', '+90']
    for col in cols_bucket: df[col] = 0.0

    def clasificar_monto(row):
        dias = row['dias_vencido']
        monto = row['amount_residual_neto']
        if dias <= 0: row['En Fecha'] = monto
        elif 1 <= dias <= 30: row['1-30'] = monto
        elif 31 <= dias <= 60: row['31-60'] = monto
        elif 61 <= dias <= 90: row['61-90'] = monto
        else: row['+90'] = monto
        return row

    df = df.apply(clasificar_monto, axis=1)
    df['date_maturity'] = df['date_maturity'].dt.date
    
    return df

def normalize_currency_code(text):
    if not text: return None
    t = str(text).lower().strip()
    if 'colon' in t or 'crc' in t: return 'CRC'
    if 'dolar' in t or 'dólar' in t or 'usd' in t: return 'USD'
    return None

def detect_currency_in_obs(obs_text):
    if not obs_text: return None
    t = str(obs_text).lower()
    if 'dolar' in t or 'dólar' in t or 'usd' in t: return 'USD'
    if 'colon' in t or 'crc' in t: return 'CRC'
    return None

def enrich_with_smart_banks_split(df, models, uid, db, password):
    if df.empty: return df
    
    partner_ids = [p for p in df['Partner_ID'].unique().tolist() if p]
    if not partner_ids: 
        df['Banco'] = ''
        df['Cuenta'] = ''
        df['Notas Banco'] = ''
        return df

    bank_domain = [
        ('partner_id', 'in', partner_ids),
        '|', ('company_id', '=', False), ('company_id', '=', 1)
    ]
    
    bank_fields = ['partner_id', 'bank_id', 'acc_number', 'x_studio_observacin', 'currency_id']
    banks_data = models.execute_kw(db, uid, password, 'res.partner.bank', 'search_read', [bank_domain], {'fields': bank_fields})
    
    banks_by_partner = {}
    for b in banks_data:
        p_id = b['partner_id'][0]
        
        banco_name = b['bank_id'][1] if b['bank_id'] else "Banco"
        cuenta_num = b['acc_number'] or ""
        obs_txt = b.get('x_studio_observacin') or ""
        moneda_obs = detect_currency_in_obs(obs_txt)
        moneda_oficial = normalize_currency_code(b['currency_id'][1] if b['currency_id'] else None)
        
        bank_obj = {
            'banco': banco_name,
            'cuenta': cuenta_num,
            'obs': obs_txt,
            'moneda_obs': moneda_obs,
            'moneda_oficial': moneda_oficial
        }
        
        if p_id not in banks_by_partner: banks_by_partner[p_id] = []
        banks_by_partner[p_id].append(bank_obj)

    def get_best_bank_columns(row):
        p_id = row['Partner_ID']
        moneda_factura = normalize_currency_code(row['Moneda'])
        default_res = pd.Series(['', '', ''], index=['Banco', 'Cuenta', 'Notas Banco'])
        
        if p_id not in banks_by_partner: return default_res
        mis_bancos = banks_by_partner[p_id]
        
        matches_obs = [b for b in mis_bancos if b['moneda_obs'] == moneda_factura]
        if matches_obs:
            best = matches_obs[0]
            return pd.Series([best['banco'], best['cuenta'], best['obs']], index=['Banco', 'Cuenta', 'Notas Banco'])

        matches_field = [b for b in mis_bancos if b['moneda_oficial'] == moneda_factura]
        if matches_field:
            best = matches_field[0]
            return pd.Series([best['banco'], best['cuenta'], best['obs']], index=['Banco', 'Cuenta', 'Notas Banco'])
        
        matches_any = [b for b in mis_bancos if b['moneda_obs'] is None and b['moneda_oficial'] is None]
        if matches_any:
            best = matches_any[0]
            return pd.Series([best['banco'], best['cuenta'], best['obs']], index=['Banco', 'Cuenta', 'Notas Banco'])
        
        if mis_bancos:
             best = mis_bancos[0]
             return pd.Series([best['banco'], best['cuenta'], best['obs']], index=['Banco', 'Cuenta', 'Notas Banco'])

        return default_res

    bank_cols = df.apply(get_best_bank_columns, axis=1)
    df = pd.concat([df, bank_cols], axis=1)
    
    return df

# ==========================================
# 2. GENERACIÓN DE EXCEL MULTI-HOJA
# ==========================================

def clasificar_factura(row):
    """Define a qué hoja va la factura según prioridad"""
    
    # Prioridad 1: Reintegros (Campo Studio activado)
    if row.get('x_studio_es_reintegro') is True:
        return 'Reintegros Cajas Chicas'
    
    # Prioridad 2: Versatec (Referencia contiene VF)
    ref = str(row.get('ref', '')).upper()
    if "VF" in ref:
        return 'Versatec'
    
    # Prioridad 3: Por Moneda
    moneda = normalize_currency_code(row.get('Moneda', ''))
    if moneda == 'USD':
        return 'Dólares'
    else:
        return 'Colones'

def generar_excel_agrupado(df):
    output = io.BytesIO()
    
    # 1. Aplicar clasificación
    df['Hoja_Destino'] = df.apply(clasificar_factura, axis=1)
    
    # Columnas a exportar
    cols_export = ['ref', 'date', 'date_maturity', 'dias_vencido', 'Moneda', 
                   'En Fecha', '1-30', '31-60', '61-90', '+90', 'amount_residual_neto', 
                   'Banco', 'Cuenta', 'Notas Banco'] 
    
    header_names = ['Referencia', 'Emisión', 'Vencimiento', 'Días Vencido', 'Moneda', 
                    'Por Vencer', '1-30 Días', '31-60 Días', '61-90 Días', '+90 Días', 'Total',
                    'Banco', 'Cuenta', 'Notas']

    # 2. Escribir Excel con múltiples hojas
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        
        # Obtenemos las categorías únicas para crear las hojas
        # Forzamos un orden específico si existen datos
        orden_hojas = ['Colones', 'Dólares', 'Reintegros Cajas Chicas', 'Versatec']
        hojas_existentes = df['Hoja_Destino'].unique().tolist()
        
        for nombre_hoja in orden_hojas:
            if nombre_hoja not in hojas_existentes:
                continue # Si no hay datos para esta hoja, la saltamos (o podrías crearla vacía)

            # Filtramos datos para esta hoja
            df_hoja = df[df['Hoja_Destino'] == nombre_hoja].copy()
            
            # Ordenamos
            df_hoja = df_hoja.sort_values(by=['Proveedor', 'dias_vencido'], ascending=[True, False])
            
            # Creamos la hoja
            workbook = writer.book
            worksheet = workbook.create_sheet(nombre_hoja)
            
            # Estilos
            bold_font = styles.Font(bold=True)
            white_font = styles.Font(bold=True, color="FFFFFF")
            header_fill = styles.PatternFill(start_color="4472C4", end_color="4472C4", fill_type="solid")
            prov_fill = styles.PatternFill(start_color="D9E1F2", end_color="D9E1F2", fill_type="solid")
            
            # Encabezados
            for col_idx, val in enumerate(header_names, 1):
                cell = worksheet.cell(row=1, column=col_idx, value=val)
                cell.font = white_font
                cell.fill = header_fill
            
            current_row = 2
            proveedores = df_hoja['Proveedor'].unique()
            
            for prov in proveedores:
                cell_title = worksheet.cell(row=current_row, column=1, value=f"PROVEEDOR: {prov}")
                cell_title.font = bold_font
                cell_title.fill = prov_fill
                worksheet.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=len(header_names))
                current_row += 1
                
                sub_df = df_hoja[df_hoja['Proveedor'] == prov][cols_export]
                
                for _, row in sub_df.iterrows():
                    for col_idx, value in enumerate(row, 1):
                        cell = worksheet.cell(row=current_row, column=col_idx, value=value)
                        if 6 <= col_idx <= 11: 
                            cell.number_format = '#,##0.00'
                    current_row += 1
                current_row += 1

            # Ajuste de ancho
            for col in worksheet.columns:
                max_len = 0
                col_letter = col[0].column_letter 
                for cell in col:
                    try:
                        if len(str(cell.value)) > max_len: max_len = len(str(cell.value))
                    except: pass
                worksheet.column_dimensions[col_letter].width = min(max_len + 2, 40)
        
        # Eliminar la hoja "Sheet" que se crea por defecto si no se usó
        if "Sheet" in writer.book.sheetnames:
            del writer.book["Sheet"]

    return output.getvalue()

# ==========================================
# 3. VISTAS
# ==========================================

def vista_inicio():
    st.title("🏠 Portal Financiero")
    st.markdown("Bienvenido. Genera tus reportes desde el menú lateral.")

def vista_reporte():
    st.title("📊 Cuentas por pagar")
    st.divider()

    col1, col2 = st.columns([1, 3])
    with col1:
        st.subheader("Configuración")
        f_corte = st.date_input("Fecha de Corte", pd.to_datetime("today"))
        btn = st.button("Generar Reporte", type="primary")

    with col2:
        if btn:
            with st.spinner('Procesando...'):
                uid, models, db, pwd = get_odoo_connection()
                if uid:
                    df = fetch_data(uid, models, db, pwd, f_corte)
                    if not df.empty:
                        df = enrich_with_smart_banks_split(df, models, uid, db, pwd)
                        
                        # --- VISTA PREVIA ---
                        # Clasificamos también para la vista previa
                        df['Hoja_Destino'] = df.apply(clasificar_factura, axis=1)
                        
                        st.subheader("Resumen General")
                        
                        # Mostramos un contador por categoría
                        resumen = df['Hoja_Destino'].value_counts()
                        st.dataframe(resumen, use_container_width=True)
                        
                        cols_view = ['Proveedor', 'ref', 'date_maturity', 'dias_vencido', 
                                     'amount_residual_neto', 'Moneda', 'Hoja_Destino']
                        
                        df_display = df.sort_values(by='dias_vencido', ascending=False)[cols_view]

                        st.dataframe(
                            df_display.style.format({'amount_residual_neto': "{:,.2f}"})
                            .map(lambda x: 'color: #d9534f' if x > 0 else 'color: black', subset=['dias_vencido']),
                            use_container_width=True
                        )
                        
                        excel_data = generar_excel_agrupado(df)
                        st.download_button("📥 Descargar Excel Multi-Hoja", excel_data, f"Antiguedad_{f_corte}.xlsx", "application/vnd.ms-excel")
                    else:
                        st.warning("No hay datos.")

# ==========================================
# 4. REPORTE VENTAS RETAIL
# ==========================================

def fetch_retail_sales(uid, models, db, password, start_date, end_date):
    """
    Trae facturas y notas de crédito de clientes (out_invoice, out_refund)
    FILTRADO POR VENDEDORES ESPECÍFICOS RETAIL.
    Lista: ALEJANDRO HERNANDEZ , GREIVIN VASQUEZ , JACKSON ABARCA , JOHNSEN MONTERO , LEONARDO CORRALES , SEBASTIAN CARRILLO
    """
    
    # 1. Traer TODAS las facturas en el rango (Company 1, Posted)
    domain_moves = [
        ('state', '=', 'posted'),
        ('company_id', '=', 1),
        ('move_type', 'in', ['out_invoice', 'out_refund']),
        ('invoice_date', '>=', str(start_date)),
        ('invoice_date', '<=', str(end_date)),
    ]
    
    fields_moves = ['name', 'invoice_date', 'invoice_user_id', 'amount_untaxed_signed', 'move_type', 'partner_id']
    
    moves = models.execute_kw(db, uid, password, 'account.move', 'search_read', [domain_moves], {'fields': fields_moves})
    
    if not moves:
        return pd.DataFrame()
        
    df = pd.DataFrame(moves)
    
    # 2. Filtrar por Vendedor (Case Insensitive Match)
    # Lista de vendedores Retail
    vendedores_retail = [
        "ALEJANDRO HERNANDEZ", 
        "GREIVIN VASQUEZ", 
        "JACKSON ABARCA", 
        "JOHNSEN MONTERO", 
        "LEONARDO CORRALES", 
        "SEBASTIAN CARRILLO"
    ]
    
    # Normalizamos para comparar (mayúsculas)
    vendedores_retail_norm = [v.upper() for v in vendedores_retail]
    
    def es_vendedor_retail(user_tuple):
        if not user_tuple: return False
        # user_tuple es (id, "Nombre")
        nombre = str(user_tuple[1]).upper()
        # Verificamos si alguno de los nombres retail está contenido en el nombre del vendedor Odoo
        # Usamos coincidencia exacta o substring según preferencia. 
        # Dado que los nombres parecen completos, intentaremos coincidencia parcial fuerte o exacta.
        for v in vendedores_retail_norm:
            if v in nombre:
                return True
        return False

    df['Vendedor_Raw'] = df['invoice_user_id']
    df = df[df['Vendedor_Raw'].apply(es_vendedor_retail)].copy()
    
    if df.empty:
        return pd.DataFrame()
    
    # 3. Procesamiento
    df['Vendedor'] = df['invoice_user_id'].apply(lambda x: x[1] if x else 'Sin Vendedor')
    df['Cliente'] = df['partner_id'].apply(lambda x: x[1] if x else 'Sin Cliente')
    df['Fecha'] = pd.to_datetime(df['invoice_date'])
    df['Mes'] = df['Fecha'].dt.strftime('%Y-%m') # Agrupación Mensual
    
    # Renombrar para visualización
    df = df.rename(columns={
        'name': 'Número Factura',
        'amount_untaxed_signed': 'Monto (CRC) Antes Imp.'
    })
    
    return df

def vista_ventas_retail():
    st.title("🛍️ Ventas Netas Retail")
    st.markdown("Facturas y Notas de Crédito confirmadas **sin cuenta analítica**.")
    st.divider()

    col1, col2 = st.columns([1, 3])
    with col1:
        st.subheader("Filtros")
        
        # Default: Mes actual
        today = datetime.date.today()
        first_day = today.replace(day=1)
        
        f_inicio = st.date_input("Fecha Inicio", first_day)
        f_fin = st.date_input("Fecha Fin", today)
        
        btn = st.button("Generar Reporte Retail", type="primary")

    with col2:
        if btn:
            with st.spinner('Consultando Odoo...'):
                uid, models, db, pwd = get_odoo_connection()
                if uid:
                    df = fetch_retail_sales(uid, models, db, pwd, f_inicio, f_fin)
                    
                    if not df.empty:
                        # KPI Totals
                        total_ventas = df['Monto (CRC) Antes Imp.'].sum()
                        
                        st.metric(label="Total Ventas Netas (Sin Impuestos)", value=f"₡ {total_ventas:,.2f}")
                        
                        # Agrupado por Mes
                        st.subheader("Desglose Mensual")
                        df_grouped = df.groupby('Mes')['Monto (CRC) Antes Imp.'].sum().reset_index()
                        st.dataframe(df_grouped.style.format({'Monto (CRC) Antes Imp.': "₡ {:,.2f}"}), use_container_width=True)
                        
                        # Detalle
                        st.subheader("Detalle de Facturas")
                        cols_view = ['Fecha', 'Mes', 'Número Factura', 'Cliente', 'Vendedor', 'Monto (CRC) Antes Imp.']
                        st.dataframe(
                            df[cols_view].sort_values(by='Fecha', ascending=False)
                            .style.format({'Monto (CRC) Antes Imp.': "{:,.2f}"}),
                            use_container_width=True
                        )
                        
                        # Excel Download
                        output = io.BytesIO()
                        with pd.ExcelWriter(output, engine='openpyxl') as writer:
                            df[cols_view].to_excel(writer, index=False, sheet_name='Detalle Retail')
                            df_grouped.to_excel(writer, index=False, sheet_name='Resumen Mensual')
                            
                        st.download_button(
                            "📥 Descargar Excel Retail", 
                            output.getvalue(), 
                            f"Ventas_Retail_{f_inicio}_al_{f_fin}.xlsx", 
                            "application/vnd.ms-excel"
                        )
                        
                    else:
                        st.info("No se encontraron registros para los filtros seleccionados (o todas las facturas tienen cuenta analítica).")

def main():
    st.sidebar.title("Menú")
    opciones = {
        "Inicio": vista_inicio, 
        "Antigüedad de Saldos": vista_reporte,
        "Ventas Retail": vista_ventas_retail
    }
    selection = st.sidebar.radio("Ir a:", list(opciones.keys()))
    opciones[selection]()

if __name__ == "__main__":
    main()


