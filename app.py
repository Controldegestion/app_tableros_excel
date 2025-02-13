import streamlit as st
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime, timedelta
import pytz
import re
from config import cargar_configuracion

# Cargar configuración
aws_access_key, aws_secret_key, region_name, bucket_name, valid_user, valid_password = cargar_configuracion()

# Configuración de AWS S3
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region_name
)

# Función para cargar un archivo en S3
def upload_file_to_s3(file, filename, original_filename):
    try:
        s3.upload_fileobj(file, bucket_name, filename)
        st.success(f"Archivo '{original_filename}' subido exitosamente.")
    except Exception as e:
        st.error(f"Error al subir el archivo: {e}")

# Función para guardar errores en un archivo log en S3
def log_error_to_s3(error_message, filename):
    try:
        log_filename = "Errores.txt"
        now = datetime.now()
        log_entry = pd.DataFrame([{
            "Fecha": now.strftime('%Y-%m-%d'),
            "Hora": now.strftime('%H:%M'),
            "Error": error_message,
            "NombreArchivo": filename
        }])
        
        # Descargar el archivo log existente si existe
        try:
            log_obj = s3.get_object(Bucket=bucket_name, Key=log_filename)
            log_df = pd.read_csv(BytesIO(log_obj['Body'].read()))
            log_df = pd.concat([log_df, log_entry], ignore_index=True)
        except s3.exceptions.NoSuchKey:
            log_df = log_entry  # Crear un nuevo DataFrame si no existe el archivo

        # Subir el archivo log actualizado
        csv_buffer = BytesIO()
        log_df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
        csv_buffer.seek(0)
        s3.put_object(Bucket=bucket_name, Key=log_filename, Body=csv_buffer.getvalue())
    except Exception as e:
        st.error(f"Error al guardar el log en S3: {e}")

# Verificar formato del nombre del archivo
def validate_filename(filename):
    pattern = r"^\d{2}-\d{2}-\d{4}\+.+\+.+\.xlsx$"
    return re.match(pattern, filename)

# Extraer el nombre del líder del archivo
def extract_leader_name(filename):
    try:
        return filename.split('+')[-1].replace('.xlsx', '')
    except IndexError:
        return None

# Extraer la fecha y la sucursal del archivo
def extract_date_and_sucursal(filename):
    try:
        parts = filename.split('+')
        fecha = parts[0]
        sucursal = parts[1]
        return fecha, sucursal
    except IndexError:
        return None, None

# Verificar celdas del formulario
def validate_form_cells(sheet_data, sheet_name, filename):
    try:
        required_cells = ['B1', 'B2', 'B3', 'B4']
        for cell in required_cells:
            if pd.isna(sheet_data.at[int(cell[1])-1, 1]):
                error_message = f"Error: La celda {cell} en la hoja '{sheet_name}' está vacía."
                st.error(error_message)
                log_error_to_s3(error_message, filename)
                return False

        cuil = str(sheet_data.at[1, 1])
        if not re.match(r"^\d{11}$", cuil):
            error_message = f"Error: La celda B2 en la hoja '{sheet_name}' debe contener 11 números."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        return True
    except Exception as e:
        error_message = f"Error al validar las celdas del formulario en la hoja '{sheet_name}': {e}"
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return False

# Verificar columnas requeridas
def validate_required_columns(data):
    required_columns = [
        'Tipo Indicador', 'Tipo Dato', 'Indicadores de Gestion', 'Ponderacion',
        'Objetivo Aceptable (70%)', 'Objetivo Muy Bueno (90%)', 'Objetivo Excelente (120%)',
        'Resultado', '% Logro', 'Calificación', 'Ultima Fecha de Actualización',
        'Lider Revisor', 'Comentario'
    ]
    missing_columns = [col for col in required_columns if col not in data.columns]
    if missing_columns:
        return False, missing_columns
    return True, []

# Función para verificar si hay ponderaciones con 0%
def validate_ponderacion(data, filename):
    if (data['Ponderacion'] == 0).any():
        error_message = "Error: Existen filas con Ponderacion 0%."
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return False
    return True

# Función para verificar si la suma de la columna Ponderacion es 1
def validate_ponderacion_sum(data, filename, sheet_name):
    ponderacion_sum = data['Ponderacion'].sum()
    if not (0.99 <= ponderacion_sum <= 1.1):
        error_message = f"Error: La suma de la columna Ponderacion en la hoja '{sheet_name}' es {ponderacion_sum * 100:.2f}%, no es 100%."
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return False
    return True

# Función para verificar estructura interna de cada hoja
def verify_sheet_structure(sheet_data, sheet_name, filename):
    if sheet_data.empty or sheet_data.shape[1] < 1:
        error_message = f"Error: La hoja '{sheet_name}' está vacía o no tiene suficientes columnas."
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return False
    return True

# Función para extraer datos del formulario
def extract_data_from_form(sheet_data):
    try:
        cargo = sheet_data.iloc[0, 1]
        cuil = sheet_data.iloc[1, 1]
        segmento = sheet_data.iloc[2, 1]
        area_influencia = sheet_data.iloc[3, 1]
        comisiones_accesorias = sheet_data.iloc[0, 10]
        hs_extras_50 = sheet_data.iloc[1, 10]
        hs_extras_100 = sheet_data.iloc[2, 10]
        incentivo_productividad = sheet_data.iloc[3, 10]
        ajuste_incentivo = sheet_data.iloc[4, 10]
        return cargo, cuil, segmento, area_influencia, comisiones_accesorias, hs_extras_50, hs_extras_100, incentivo_productividad, ajuste_incentivo
    except IndexError:
        return None, None, None, None, None, None, None, None, None

# Función para contar filas hasta encontrar una vacía
def count_rows_until_empty(data, column_name="Indicadores de Gestion"):
    try:
        header_index = data[data.iloc[:, 2] == column_name].index[0]
        relevant_rows = data.iloc[header_index + 1:, 2]
        return relevant_rows.isna().idxmax() - (header_index + 1)
    except Exception as e:
        st.error(f"Error contando filas hasta vacío: {e}")
        return 0

# Función para limpiar y reestructurar datos
def clean_and_restructure_until_empty(data, cargo, cuil, segmento, area_influencia, leader_name, fecha, sucursal, filename, upload_datetime, sheet_name, comisiones_accesorias, hs_extras_50, hs_extras_100, incentivo_productividad, ajuste_incentivo):
    try:
        header_row = data[data.iloc[:, 0] == 'Tipo Indicador'].index[0]
        rows_to_process = count_rows_until_empty(data, "Indicadores de Gestion")

        if rows_to_process == 0:
            error_message = "Error: No se encontraron filas válidas después del encabezado."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return pd.DataFrame()

        data.columns = data.iloc[header_row]
        data = data.iloc[header_row + 1:header_row + 1 + rows_to_process].reset_index(drop=True)

        column_mapping = {
            'Tipo Indicador': 'Tipo Indicador',
            'Tipo Dato': 'Tipo Dato',
            'Indicadores de Gestion': 'Indicadores de Gestion',
            'Ponderacion': 'Ponderacion',
            'Objetivo Aceptable (70%)': 'Objetivo Aceptable (70%)',
            'Objetivo Muy Bueno (90%)': 'Objetivo Muy Bueno (90%)',
            'Objetivo Excelente (120%)': 'Objetivo Excelente (120%)',
            'Resultado': 'Resultado',
            '% Logro': '% Logro',
            'Calificación': 'Calificación',
            'Ultima Fecha de Actualización': 'Ultima Fecha de Actualización',
            'Lider Revisor': 'Lider Revisor',
            'Comentario': 'Comentario'
        }
        data = data.rename(columns=column_mapping)

        valid_columns, missing_columns = validate_required_columns(data)
        if not valid_columns:
            error_message = f"Error: Faltan las siguientes columnas requeridas: {', '.join(missing_columns)}"
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return pd.DataFrame()

        if not validate_ponderacion(data, filename):
            return pd.DataFrame()

        if not validate_ponderacion_sum(data, filename, sheet_name):
            return pd.DataFrame()

        data['Cargo'] = cargo
        data['CUIL'] = cuil
        data['Segmento'] = segmento
        data['Área de influencia'] = area_influencia
        data['Nombre Lider'] = leader_name
        data['Fecha_Nombre_Archivo'] = fecha
        data['Sucursal'] = sucursal
        data['Fecha Horario Subida'] = upload_datetime
        data['COMISIONES ACCESORIAS'] = comisiones_accesorias
        data['HS EXTRAS AL 50'] = hs_extras_50
        data['HS EXTRAS AL 100'] = hs_extras_100
        data['INCENTIVO PRODUCTIVIDAD'] = incentivo_productividad
        data['AJUSTE INCENTIVO'] = ajuste_incentivo

        desired_columns = [
            'Cargo', 'CUIL', 'Segmento', 'Área de influencia', 'Nombre Lider', 'Fecha_Nombre_Archivo', 'Sucursal',
            'Fecha Horario Subida', 'Tipo Indicador', 'Tipo Dato', 'Indicadores de Gestion', 'Ponderacion',
            'Objetivo Aceptable (70%)', 'Objetivo Muy Bueno (90%)', 'Objetivo Excelente (120%)',
            'Resultado', '% Logro', 'Calificación',
            'Ultima Fecha de Actualización', 'Lider Revisor', 'Comentario',
            'COMISIONES ACCESORIAS', 'HS EXTRAS AL 50', 'HS EXTRAS AL 100', 'INCENTIVO PRODUCTIVIDAD', 'AJUSTE INCENTIVO'
        ]
        return data[desired_columns]
    except Exception as e:
        error_message = f"Error al limpiar y reestructurar: {e}"
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return pd.DataFrame()

# Función para verificar si hay CUILs repetidos en diferentes hojas
def validate_unique_cuils(dataframes):
    cuils = []
    for df in dataframes:
        cuils.extend(df['CUIL'].unique())
    if len(cuils) != len(set(cuils)):
        return False
    return True

# Función para procesar hojas del Excel
def process_sheets_until_empty(excel_data, filename, upload_datetime):
    final_data = pd.DataFrame()
    leader_name = extract_leader_name(filename)
    fecha, sucursal = extract_date_and_sucursal(filename)
    dataframes = []
    for sheet_name in excel_data.sheet_names:
        sheet_data = excel_data.parse(sheet_name, header=None)
        if not verify_sheet_structure(sheet_data, sheet_name, filename):
            return pd.DataFrame(), False  # Return empty DataFrame and error state
        if not validate_form_cells(sheet_data, sheet_name, filename):
            return pd.DataFrame(), False  # Return empty DataFrame and error state
        cargo, cuil, segmento, area_influencia, comisiones_accesorias, hs_extras_50, hs_extras_100, incentivo_productividad, ajuste_incentivo = extract_data_from_form(sheet_data)
        if cargo and cuil and segmento and area_influencia:
            processed_data = clean_and_restructure_until_empty(sheet_data, cargo, cuil, segmento, area_influencia, leader_name, fecha, sucursal, filename, upload_datetime, sheet_name, comisiones_accesorias, hs_extras_50, hs_extras_100, incentivo_productividad, ajuste_incentivo)
            if processed_data.empty:
                return pd.DataFrame(), False  # Return empty DataFrame and error state
            dataframes.append(processed_data)
            final_data = pd.concat([final_data, processed_data], ignore_index=True)
    
    if not validate_unique_cuils(dataframes):
        error_message = "Error: Existen CUILs repetidos en diferentes hojas del archivo."
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return pd.DataFrame(), False  # Return empty DataFrame and error state

    return final_data, True  # Return DataFrame and success state

# Función para determinar si el tablero es "Ajuste" o "Normal"
def determine_tablero_type(fecha, upload_datetime):
    fecha_tablero = datetime.strptime(fecha, '%d-%m-%Y')
    limite_normal = fecha_tablero + timedelta(days=50)  # 20 días del mes siguiente
    if upload_datetime > limite_normal:
        return "Ajuste"
    return "Normal"

# Función para procesar y subir el Excel
def process_and_upload_excel(file, original_filename):
    try:
        if not validate_filename(original_filename):
            error_message = "El nombre del archivo no cumple con el formato requerido (dd-mm-aaaa+empresa+nombre lider.xlsx)."
            st.error(error_message)
            log_error_to_s3(error_message, original_filename)
            return

        excel_data = pd.ExcelFile(file)
        argentina_tz = pytz.timezone("America/Argentina/Buenos_Aires")
        now = datetime.now(argentina_tz)
        upload_datetime = now.strftime('%d/%m/%Y_%H:%M:%S')
        cleaned_df, success = process_sheets_until_empty(excel_data, original_filename, upload_datetime)

        if not success:
            error_message = "El archivo contiene errores en su estructura y no se cargará"
            st.error(error_message)
            log_error_to_s3(error_message, original_filename)
            return

        if cleaned_df.empty:
            error_message = "El archivo no tiene datos válidos después de la limpieza."
            st.error(error_message)
            log_error_to_s3(error_message, original_filename)
            return

        fecha, _ = extract_date_and_sucursal(original_filename)
        upload_datetime_obj = datetime.strptime(upload_datetime, '%d/%m/%Y_%H:%M:%S')
        tablero_type = determine_tablero_type(fecha, upload_datetime_obj)
        ajuste_value = "SI" if tablero_type == "Ajuste" else "NO"
        cleaned_df["Ajuste"] = ajuste_value

        if tablero_type == "Ajuste":
            st.warning("El tablero se va a cargar como ajuste, ¿desea guardarlo igualmente?")
            guardar = st.button("Guardar")
            cancelar = st.button("Cancelar")
            if cancelar:
                st.info("El archivo no se guardó.")
                return
            if not guardar:
                return

        csv_buffer = BytesIO()
        cleaned_df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")

        csv_filename = f"{now.strftime('%Y-%m-%d_%H-%M-%S')}_{original_filename.split('.')[0]}.csv"

        csv_buffer.seek(0)
        upload_file_to_s3(csv_buffer, csv_filename, original_filename)
    except Exception as e:
        error_message = f"Error al procesar el archivo Excel: {e}"
        st.error(error_message)
        log_error_to_s3(error_message, original_filename)

# Función principal de la aplicación
def main():
    st.title("Gestión de Tableros")

    st.header("Sube un Tablero")
    uploaded_file = st.file_uploader("Selecciona un archivo Excel", type=["xlsx"])

    if uploaded_file is not None:
        process_and_upload_excel(uploaded_file, uploaded_file.name)

if __name__ == "__main__":
    main()