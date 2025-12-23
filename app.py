import streamlit as st
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import pytz
import re
from config import cargar_configuracion
from botocore.exceptions import ClientError
from pandas.errors import EmptyDataError

# Cargar configuración
aws_access_key, aws_secret_key, region_name, bucket_name, valid_user, valid_password = cargar_configuracion()

# Configuración de AWS S3
s3 = boto3.client(
    's3',
    aws_access_key_id=aws_access_key,
    aws_secret_access_key=aws_secret_key,
    region_name=region_name
)

# Key helper para el índice por período
def _get_period_index_key(periodo_str):
    """
    Devuelve la key S3 del índice del período dentro de la carpeta del mes.
    Ejemplo: periodo_str = '01-10-2025' -> '01-10-2025/indice.csv'
    """
    return f"{periodo_str}/indice.csv"

def _load_period_index(periodo_str):
    """
    Lee el índice del período (Periodo/indice.csv).
    Si no existe o está vacío, devuelve un DataFrame vacío con columnas estándar.
    """
    key = _get_period_index_key(periodo_str)
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        try:
            df = pd.read_csv(BytesIO(obj["Body"].read()), dtype={"CUIL": str})
        except EmptyDataError:
            return pd.DataFrame(columns=["Periodo", "CUIL", "Lider"])

        expected = ["Periodo", "CUIL", "Lider"]
        for col in expected:
            if col not in df.columns:
                df[col] = None
        df = df[expected]
        return df
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code")
        if code in ("NoSuchKey", "404", "NoSuchBucket"):
            return pd.DataFrame(columns=["Periodo", "CUIL", "Lider"])
        raise

def _save_period_index(df, periodo_str):
    """
    Guarda el DataFrame del índice en '<Periodo>/indice.csv'.
    """
    key = _get_period_index_key(periodo_str)
    csv_buffer = BytesIO()
    df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
    csv_buffer.seek(0)
    s3.put_object(Bucket=bucket_name, Key=key, Body=csv_buffer.getvalue())

# Función para cargar un archivo en S3
def upload_file_to_s3(file, filename, original_filename):
    try:
        s3.upload_fileobj(file, bucket_name, filename)
        st.success(f"Archivo '{original_filename}' subido exitosamente.")
        return True
    except Exception as e:
        st.error(f"Error al subir el archivo: {e}")
        return False

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
            try:
                log_df = pd.read_csv(BytesIO(log_obj['Body'].read()))
            except Exception:
                # Si el archivo existe pero está corrupto o vacío, reiniciamos
                log_df = pd.DataFrame(columns=["Fecha", "Hora", "Error", "NombreArchivo"])
            log_df = pd.concat([log_df, log_entry], ignore_index=True)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code")
            if code in ("NoSuchKey", "404", "NoSuchBucket"):
                log_df = log_entry  # Crear nuevo si no existe
            else:
                raise

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

# Función para validar la fecha del archivo
def validate_file_date(filename):
    try:
        file_date_str = filename.split('+')[0]
        file_date = datetime.strptime(file_date_str, '%d-%m-%Y')
        now = datetime.now()
        current_month = now.month
        current_year = now.year

        # No aceptar archivos del mes actual
        if file_date.year == current_year and file_date.month == current_month:
            return False

        # Mes anterior
        prev_month = current_month - 1 if current_month > 1 else 12
        prev_year = current_year if current_month > 1 else current_year - 1
        if file_date.year == prev_year and file_date.month == prev_month:
            return True

        # Dos meses atrás, solo hasta el día 10 del mes actual
        two_months_ago = current_month - 2 if current_month > 2 else 12 + (current_month - 2)
        two_months_ago_year = current_year if current_month > 2 else current_year - 1
        # Configuracion Ajuste
        if (file_date.year == two_months_ago_year and file_date.month == two_months_ago and now.day <= 10):
            return True

        return False
    except Exception as e:
        st.error(f"Error al validar la fecha del archivo: {e}")
        return False

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

        # Validar que los campos de comisiones y horas extra sean números o nulos
        comisiones_accesorias = sheet_data.at[0, 10]
        hs_extras_50 = sheet_data.at[1, 10]
        hs_extras_100 = sheet_data.at[2, 10]
        incentivo_productividad = sheet_data.at[3, 10]
        ajuste_incentivo = sheet_data.at[4, 10]

        if not (pd.isna(comisiones_accesorias) or (isinstance(comisiones_accesorias, (int, float)) and float(comisiones_accesorias).is_integer())):
            error_message = f"Error: La celda K1 en la hoja '{sheet_name}' debe contener un número entero."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        if not (pd.isna(hs_extras_50) or isinstance(hs_extras_50, (int, float))):
            error_message = f"Error: La celda K2 en la hoja '{sheet_name}' debe contener solo números."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        if not (pd.isna(hs_extras_100) or isinstance(hs_extras_100, (int, float))):
            error_message = f"Error: La celda K3 en la hoja '{sheet_name}' debe contener solo números."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        if not (pd.isna(incentivo_productividad) or (isinstance(incentivo_productividad, (int, float)) and float(incentivo_productividad).is_integer())):
            error_message = f"Error: La celda K4 en la hoja '{sheet_name}' debe contener un número entero."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        if not (pd.isna(ajuste_incentivo) or (isinstance(ajuste_incentivo, (int, float)) and float(ajuste_incentivo).is_integer())):
            error_message = f"Error: La celda K5 en la hoja '{sheet_name}' debe contener un número entero."
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
        # Si cargo contiene una coma, lo envuelve entre comillas
        if isinstance(cargo, str) and ',' in cargo:
            cargo = f'"{cargo}"'
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

        # Validar que los objetivos sean numéricos (entero, decimal o porcentaje)
        objetivo_cols = [
            'Objetivo Aceptable (70%)',
            'Objetivo Muy Bueno (90%)',
            'Objetivo Excelente (120%)'
        ]
        for col in objetivo_cols:
            # Intentar convertir a numérico, si falla o hay texto, marcar error
            def is_valid_objetivo(x):
                if pd.isna(x) or isinstance(x, (int, float)):
                    return True
                if isinstance(x, str):
                    return bool(re.match(r"^\s*-?\d+(\.\d+)?\s*%?\s*$", x))
                return False
            invalid_mask = ~data[col].apply(is_valid_objetivo)
            if invalid_mask.any():
                error_message = (f"Error: La columna '{col}' contiene valores no numéricos o texto en la hoja '{sheet_name}'.")
                st.error(error_message)
                log_error_to_s3(error_message, filename)
                return pd.DataFrame()
            # Convertir strings con % o números a float
            def parse_objetivo(val):
                if pd.isna(val):
                    return val
                if isinstance(val, (int, float)):
                    return float(val)
                if isinstance(val, str):
                    val = val.strip().replace(",", ".")
                    if val.endswith("%"):
                        try:
                            return float(val.rstrip("%").strip()) / 100
                        except:
                            return None
                    try:
                        return float(val)
                    except:
                        return None
                return None
            data[col] = data[col].apply(parse_objetivo)
            # Si después de convertir hay algún valor que NO sea numérico y NO sea nulo, es error
            if not data[col].dropna().apply(lambda x: isinstance(x, float)).all():
                error_message = (f"Error: La columna '{col}' contiene valores no numéricos válidos en la hoja '{sheet_name}'.")
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
            if not validate_update_dates(processed_data, filename, sheet_name):
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
    # Fecha de ajuste
    ajuste_fecha = datetime(upload_datetime.year, upload_datetime.month, 21)
    file_date = datetime.strptime(fecha, '%d-%m-%Y')
    now = upload_datetime

    # Mes actual
    if file_date.month == now.month and file_date.year == now.year:
        return "Normal"

    # Mes anterior
    prev_month = now.month - 1 if now.month > 1 else 12
    prev_year = now.year if now.month > 1 else now.year - 1
    if file_date.month == prev_month and file_date.year == prev_year:
        if upload_datetime > ajuste_fecha:
            return "Ajuste"
        else:
            return "Normal"

    # Dos meses atrás (siempre ajuste)
    two_months_ago = now.month - 2 if now.month > 2 else 12 + (now.month - 2)
    two_months_ago_year = now.year if now.month > 2 else now.year - 1
    if file_date.month == two_months_ago and file_date.year == two_months_ago_year:
        return "Ajuste"

    return "Normal"

# Función para verificar fechas en la columna "Ultima Fecha de Actualización"
def validate_update_dates(data, filename, sheet_name):
    try:
        argentina_tz = pytz.timezone("America/Argentina/Buenos_Aires")
        now = datetime.now(argentina_tz)
        now = pd.to_datetime(now.strftime('%Y-%m-%d'))  # Convertir la fecha actual al mismo tipo de datos

        # Verificar si la columna existe
        if 'Ultima Fecha de Actualización' not in data.columns:
            error_message = f"Error: La columna 'Ultima Fecha de Actualización' no existe en la hoja '{sheet_name}'."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        # Verificar valores nulos
        if data['Ultima Fecha de Actualización'].isna().any():
            error_message = f"Error: Existen valores nulos en la columna 'Ultima Fecha de Actualización' en la hoja '{sheet_name}'."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        # Verificar formato de fecha
        data['Ultima Fecha de Actualización'] = pd.to_datetime(
            data['Ultima Fecha de Actualización'], format='%d/%m/%Y', errors='coerce'
        )
        if data['Ultima Fecha de Actualización'].isna().any():
            error_message = f"Error: Existen valores en la columna 'Ultima Fecha de Actualización' en la hoja '{sheet_name}' que no tienen el formato de fecha válido (%d/%m/%Y)."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        # Verificar fechas futuras
        invalid_dates = data[data['Ultima Fecha de Actualización'] > now]
        if not invalid_dates.empty:
            error_message = f"Error: Existen fechas en la columna 'Ultima Fecha de Actualización' en la hoja '{sheet_name}' que son posteriores a la fecha actual."
            st.error(error_message)
            log_error_to_s3(error_message, filename)
            return False

        return True
    except Exception as e:
        error_message = f"Error al validar las fechas en la columna 'Ultima Fecha de Actualización' en la hoja '{sheet_name}': {e}"
        st.error(error_message)
        log_error_to_s3(error_message, filename)
        return False

# Función para verificar duplicados en S3
def check_for_duplicates(cuils, fecha_normalizada, leader_name):
    """
    Verifica duplicados consultando el índice del período para una lista de CUILs.
    Devuelve:
        (is_duplicate: bool, conflicts: list[(cuil, existing_leader)])
    Bloquea si algún CUIL ya fue cargado por OTRO líder en el mismo período.
    """
    try:
        periodo = normalize_fecha_to_first_day(fecha_normalizada)  # '01-mm-aaaa'
        idx_df = _load_period_index(periodo)

        if idx_df.empty:
            return False, []  # no hay índice -> no hay conflictos

        conflicts = []
        # normalizo a str para evitar problemas de comparación
        idx_df["CUIL"] = idx_df["CUIL"].astype(str)
        cuils_str = [str(c) for c in cuils]

        for c in cuils_str:
            row = idx_df.loc[idx_df["CUIL"] == c]
            if not row.empty:
                existing_leader = (None if row["Lider"].isna().iloc[0]
                                   else str(row["Lider"].iloc[0]))
                if existing_leader and existing_leader != leader_name:
                    conflicts.append((c, existing_leader))

        return (len(conflicts) > 0), conflicts
    except Exception as e:
        st.error(f"Error al verificar duplicados en índice del período: {e}")
        return False, []

def _update_period_index_with_upload(periodo_str, cuils, leader_name):
    """
    Agrega todos los CUILs al índice '<Periodo>/indice.csv' en una sola operación:
    - Si el CUIL no existe: se agrega (Periodo, CUIL, Lider).
    - Si existe con el mismo líder: no hace nada.
    - Si existe con OTRO líder: no lo pisa (esto ya debería haberse bloqueado antes).
    """
    try:
        df = _load_period_index(periodo_str)
        df["CUIL"] = df["CUIL"].astype(str) if "CUIL" in df.columns and not df.empty else df.get("CUIL", pd.Series(dtype=str))

        to_add = []
        cuils_str = [str(c) for c in cuils]

        for c in cuils_str:
            if not df.empty and "CUIL" in df.columns:
                mask = df["CUIL"] == c
            else:
                mask = pd.Series([], dtype=bool)

            if mask.any():
                existing_leader = str(df.loc[mask, "Lider"].iloc[0]) if not df.loc[mask, "Lider"].isna().iloc[0] else None
                if existing_leader == leader_name:
                    continue  # ya está con el mismo líder
                else:
                    # existe con otro líder -> no se sobreescribe (ya fue validado)
                    continue
            else:
                to_add.append({"Periodo": periodo_str, "CUIL": c, "Lider": leader_name})

        if to_add:
            df = pd.concat([df, pd.DataFrame(to_add)], ignore_index=True)

        _save_period_index(df, periodo_str)
    except Exception as e:
        st.error(f"Error al actualizar el índice del período: {e}")

# Función para procesar y subir el Excel
def process_and_upload_excel(file, original_filename):
    try:
        if not validate_filename(original_filename):
            error_message = "El nombre del archivo no cumple con el formato requerido (dd-mm-aaaa+empresa+nombre lider.xlsx)."
            st.error(error_message)
            log_error_to_s3(error_message, original_filename)
            return

        # if not validate_file_date(original_filename):
        #     error_message = "La fecha del nombre del archivo solo puede ser de un mes anterior, o de dos meses atrás (hasta el día 10)."
        #     st.error(error_message)
        #     log_error_to_s3(error_message, original_filename)
        #     return

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

        # ===== CUILs únicos del archivo =====
        unique_cuils = cleaned_df['CUIL'].astype(str).unique().tolist()
        fecha_archivo = original_filename.split('+')[0]                # ej: '03-04-2025'
        periodo_str = normalize_fecha_to_first_day(fecha_archivo)      # ej: '01-04-2025'
        leader_name = cleaned_df['Nombre Lider'].iloc[0]

        # Verificar duplicados usando ÍNDICE DEL PERÍODO para TODOS los CUILs
        is_duplicate, conflicts = check_for_duplicates(unique_cuils, periodo_str, leader_name)
        if is_duplicate:
            conflictos_txt = "\n".join([f"- CUIL {cuil} ya subido por '{lider}'" for cuil, lider in conflicts])
            error_message = (
                "No se puede subir el archivo porque existen CUILs ya cargados por otro líder en el período:\n"
                f"{conflictos_txt}"
            )
            st.error(error_message)
            log_error_to_s3(error_message, original_filename)
            return

        # Contar CUILs únicos (tableros)
        unique_cuils_count = len(unique_cuils)
        st.info(f"Se subieron {unique_cuils_count} tableros.")

        upload_datetime_obj = datetime.strptime(upload_datetime, '%d/%m/%Y_%H:%M:%S')
        tablero_type = determine_tablero_type(fecha_archivo, upload_datetime_obj)
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

        # Armado ruta destino del CSV limpio
        fecha_carpeta = periodo_str  # ya normalizada a '01-mm-aaaa'
        csv_filename = f"{fecha_carpeta}/{now.strftime('%Y-%m-%d_%H-%M-%S')}_{original_filename.split('.')[0]}.csv"

        # Subir CSV limpio a S3
        csv_buffer = BytesIO()
        cleaned_df.to_csv(csv_buffer, index=False, encoding="utf-8-sig")
        csv_buffer.seek(0)
        ok = upload_file_to_s3(csv_buffer, csv_filename, original_filename)
        if not ok:
            # Si falló la subida del CSV, no toco el índice
            return

        # ✅ Actualizar índice del período con TODOS los CUILs subidos
        _update_period_index_with_upload(periodo_str, unique_cuils, leader_name)

    except Exception as e:
        error_message = f"Error al procesar el archivo Excel: {e}"
        st.error(error_message)
        log_error_to_s3(error_message, original_filename)

def normalize_fecha_to_first_day(fecha_str):
    """Convierte cualquier fecha dd-mm-aaaa a 01-mm-aaaa"""
    try:
        dt = datetime.strptime(fecha_str, "%d-%m-%Y")
        return dt.replace(day=1).strftime("%d-%m-%Y")
    except Exception:
        return fecha_str  # Si falla, devuelve la original
    
# Función principal de la aplicación
def main():
    st.title("Gestión de Tableros")

    st.header("Sube un Tablero")
    uploaded_file = st.file_uploader("Selecciona un archivo Excel", type=["xlsx"])

    if uploaded_file is not None:
        process_and_upload_excel(uploaded_file, uploaded_file.name)

if __name__ == "__main__":
    main()



