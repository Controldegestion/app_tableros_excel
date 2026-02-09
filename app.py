import streamlit as st

# Elegí acá qué versión querés ejecutar
ACTIVE_VERSION = "v2"  # "v1" o "v2"

st.write(f"VERSIÓN EN PRODUCCIÓN: {ACTIVE_VERSION}")

if ACTIVE_VERSION == "v1":
    from app_v1 import main
elif ACTIVE_VERSION == "v2":
    from app_v2 import main
else:
    st.error("ACTIVE_VERSION inválida. Usá 'v1' o 'v2'.")
    raise ValueError("ACTIVE_VERSION inválida")

main()



