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
    domain = [
        ('parent_state', '=', 'posted'),
        ('company_id', '=', 1),  # <--- FILTRO AGREGADO: Solo Company ID 1
        ('account_type', '=', 'liability_payable'),
        ('amount_residual', '!=', 0),
        ('move_id.move_type', 'in', ['in_invoice', 'in_refund']),
    ]
    
    fields = ['partner_id', 'date_maturity', 'date', 'ref', 'amount_residual', 'currency_id', 'move_id']
    lines = models.execute_kw(db, uid, password, 'account.move.line', 'search_read', [domain], {'fields': fields})
    
    if not lines: return pd.DataFrame()

    df = pd.DataFrame(lines)
    
    # 2. Identificar tipos (Factura vs NC)
    move_ids_list = [m[0] for m in df['move_id'] if m]
    move_ids_unique = list(set(move_ids_list))
    
    type_map = {}
    if move_ids_unique:
        moves_data = models.execute_kw(db, uid, password, 'account.move', 'search_read', 
                                       [[('id', 'in', move_ids_unique)]], 
                                       {'fields': ['move_type']})
        type_map = {m['id']: m['move_type'] for m in moves_data}

    # 3. Limpieza
    df['Proveedor'] = df['partner_id'].apply(lambda x: x[1] if x else 'Sin Proveedor')
    df['Partner_ID'] = df['partner_id'].apply(lambda x: x[0] if x else False)
    df['Moneda'] = df['currency_id'].apply(lambda x: x[1] if x else '')
    df['ref'] = df['ref'].apply(lambda x: x if x else '-')
    df['move_id_int'] = df['move_id'].apply(lambda x: x[0] if x else False)
    
    # Corrección Fechas Vacías
    df['date_maturity'] = df.apply(lambda row: row['date'] if not row['date_maturity'] else row['date_maturity'], axis=1)
    df['date_maturity'] = pd.to_datetime(df['date_maturity'], errors='coerce')
    df = df.dropna(subset=['date_maturity'])

    # 4. Signos
    def calcular_neto(row):
        tipo = type_map.get(row['move_id_int'], 'in_invoice')
        monto = abs(row['amount_residual'])
        if tipo == 'in_refund': return -monto
        return monto

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
    """Convierte texto general a código estandar USD/CRC"""
    if not text: return None
    t = str(text).lower().strip()
    if 'colon' in t or 'crc' in t: return 'CRC'
    if 'dolar' in t or 'dólar' in t or 'usd' in t: return 'USD'
    return None

def detect_currency_in_obs(obs_text):
    """Busca palabras clave dentro de una frase larga (Observaciones)"""
    if not obs_text: return None
    t = str(obs_text).lower()
    # Prioridad de búsqueda
    if 'dolar' in t or 'dólar' in t or 'usd' in t: return 'USD'
    if 'colon' in t or 'crc' in t: return 'CRC'
    return None

def enrich_with_smart_banks_split(df, models, uid, db, password):
    """
    Busca bancos priorizando 'Observaciones' y filtrando por Compañía 1 o Compartidos.
    """
    if df.empty: return df
    
    partner_ids = [p for p in df['Partner_ID'].unique().tolist() if p]
    if not partner_ids: 
        df['Banco'] = ''
        df['Cuenta'] = ''
        df['Notas Banco'] = ''
        return df

    # FILTRO: Partner correcto Y (Compañía 1 O Compañía "Vacía/Compartida")
    bank_domain = [
        ('partner_id', 'in', partner_ids),
        '|', ('company_id', '=', False), ('company_id', '=', 1) # <--- FILTRO NUEVO
    ]
    
    bank_fields = ['partner_id', 'bank_id', 'acc_number', 'x_studio_observacin', 'currency_id']
    banks_data = models.execute_kw(db, uid, password, 'res.partner.bank', 'search_read', [bank_domain], {'fields': bank_fields})
    
    # Organizar bancos
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
        
        if p_id not in banks_by_partner: 
            return default_res
            
        mis_bancos = banks_by_partner[p_id]
        
        # 1. PRIORIDAD MÁXIMA: Coincidencia por OBSERVACIONES
        matches_obs = [b for b in mis_bancos if b['moneda_obs'] == moneda_factura]
        if matches_obs:
            best = matches_obs[0]
            return pd.Series([best['banco'], best['cuenta'], best['obs']], index=['Banco', 'Cuenta', 'Notas Banco'])

        # 2. PRIORIDAD MEDIA: Coincidencia por CAMPO MONEDA
        matches_field = [b for b in mis_bancos if b['moneda_oficial'] == moneda_factura]
        if matches_field:
            best = matches_field[0]
            return pd.Series([best['banco'], best['cuenta'], best['obs']], index=['Banco', 'Cuenta', 'Notas Banco'])
        
        # 3. PRIORIDAD BAJA: Comodines
        matches_any = [b for b in mis_bancos if b['moneda_obs'] is None and b['moneda_oficial'] is None]
        if matches_any:
            best = matches_any[0]
            return pd.Series([best['banco'], best['cuenta'], best['obs']], index=['Banco', 'Cuenta', 'Notas Banco'])
        
        # 4. ÚLTIMO RECURSO
        if mis_bancos:
             best = mis_bancos[0]
             return pd.Series([best['banco'], best['cuenta'], best['obs']], index=['Banco', 'Cuenta', 'Notas Banco'])

        return default_res

    bank_cols = df.apply(get_best_bank_columns, axis=1)
    df = pd.concat([df, bank_cols], axis=1)
    
    return df
# ==========================================
# 2. GENERACIÓN DE EXCEL
# ==========================================

def generar_excel_agrupado(df):
    output = io.BytesIO()
    df_sorted = df.sort_values(by=['Proveedor', 'dias_vencido'], ascending=[True, False])
    
    cols_export = ['ref', 'date', 'date_maturity', 'dias_vencido', 'Moneda', 
                   'En Fecha', '1-30', '31-60', '61-90', '+90', 'amount_residual_neto', 
                   'Banco', 'Cuenta', 'Notas Banco'] 
    
    header_names = ['Referencia', 'Emisión', 'Vencimiento', 'Días Vencido', 'Moneda', 
                    'Por Vencer', '1-30 Días', '31-60 Días', '61-90 Días', '+90 Días', 'Total',
                    'Banco', 'Cuenta', 'Notas'] 

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        workbook = writer.book
        worksheet = workbook.create_sheet("Antigüedad de Saldos")
        
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
        proveedores = df_sorted['Proveedor'].unique()
        
        for prov in proveedores:
            cell_title = worksheet.cell(row=current_row, column=1, value=f"PROVEEDOR: {prov}")
            cell_title.font = bold_font
            cell_title.fill = prov_fill
            worksheet.merge_cells(start_row=current_row, start_column=1, end_row=current_row, end_column=len(header_names))
            current_row += 1
            
            sub_df = df_sorted[df_sorted['Proveedor'] == prov][cols_export]
            
            for _, row in sub_df.iterrows():
                for col_idx, value in enumerate(row, 1):
                    cell = worksheet.cell(row=current_row, column=col_idx, value=value)
                    if 6 <= col_idx <= 11: 
                        cell.number_format = '#,##0.00'
                current_row += 1
            current_row += 1

        for col in worksheet.columns:
            max_len = 0
            col_letter = col[0].column_letter 
            for cell in col:
                try:
                    if len(str(cell.value)) > max_len: max_len = len(str(cell.value))
                except: pass
            worksheet.column_dimensions[col_letter].width = min(max_len + 2, 40)

    return output.getvalue()

# ==========================================
# 3. VISTAS
# ==========================================

def vista_inicio():
    st.title("🏠 Portal Financiero")
    st.markdown("Bienvenido. Genera tus reportes desde el menú lateral.")

def vista_reporte():
    st.title("📊 CXP Antigüedad de Saldos")
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
                        # Lógica mejorada de bancos
                        df = enrich_with_smart_banks_split(df, models, uid, db, pwd)
                        
                        st.subheader("Vista Previa")
                        
                        cols_view = ['Proveedor', 'ref', 'date_maturity', 'dias_vencido', 
                                     'amount_residual_neto', 'Moneda', 
                                     'Banco', 'Cuenta', 'Notas Banco']
                        
                        df_display = df.sort_values(by='dias_vencido', ascending=False)[cols_view]

                        st.dataframe(
                            df_display.style.format({'amount_residual_neto': "{:,.2f}"})
                            .map(lambda x: 'color: #d9534f' if x > 0 else 'color: black', subset=['dias_vencido']),
                            use_container_width=True
                        )
                        
                        excel_data = generar_excel_agrupado(df)
                        st.download_button("📥 Descargar Excel Agrupado", excel_data, f"Antiguedad_{f_corte}.xlsx", "application/vnd.ms-excel")
                    else:
                        st.warning("No hay datos.")

def main():
    st.sidebar.title("Menú")
    opciones = {"Inicio": vista_inicio, "CXP Antigüedad de Saldos": vista_reporte}
    selection = st.sidebar.radio("Ir a:", list(opciones.keys()))
    opciones[selection]()

if __name__ == "__main__":

    main()


