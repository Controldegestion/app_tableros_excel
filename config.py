import json
import os
import streamlit as st

def cargar_configuracion():

    # # Configuracion Local
    # # Cargar configuración desde el archivo config.json
    # with open("../config.json") as config_file:
    #     config = json.load(config_file)

    # # Desempaquetar las credenciales desde el archivo de configuración
    # aws_access_key = config["aws_access_key"]
    # aws_secret_key = config["aws_secret_key"]
    # region_name = config["region_name"]
    # bucket_name = config["bucket_name"]
    # users = config["users"]
    # passwords = config["passwords"]

    #Configuracion Streamlit
    aws_access_key = st.secrets["aws_access_key"]
    aws_secret_key = st.secrets["aws_secret_key"]
    region_name = st.secrets["region_name"]
    bucket_name = st.secrets["bucket_name"]
    users = st.secrets["users"]
    passwords = st.secrets["passwords"]

    return aws_access_key, aws_secret_key, region_name, bucket_name, users, passwords