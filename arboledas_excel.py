import pandas as pd
import streamlit as st
import numpy as np
from google.oauth2 import service_account
from google.cloud import bigquery
import base64

st.title('Parque Arboleda')


# Create API client.
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

uploaded_file = st.file_uploader('Choose an excel file', type=['xlsx', 'xls'])

# Condicional y garantia de lectura
if uploaded_file is not None:
    file_extension = uploaded_file.name.split('.')[-1]

    if file_extension == 'xlsx':
        df = pd.read_excel(uploaded_file, engine='openpyxl')
    elif file_extension == 'xls':
        df = pd.read_excel(uploaded_file, engine='xlrd')
    else:
        st.error('Porfavor sube un archivo excel')

# Drop la primer fila que ahora es el header
df = df[1:]

# Mapeo proporcionado por Arboleda
etiquetas_salida = {24: 'Salida 1', 26: 'Salida 2', 28: 'Salida 3', 32: 'Salida 4', 44: 'Salida 6', 22: 'Salida 7', 48: 'Salida 8', 50: 'Salida 9'}

df['Etiqueta Salida'] = df['Dispositivo'].map(etiquetas_salida)

df['Fecha/Hora'] = pd.to_datetime(df['Fecha/Hora'])

df['Fecha'] = df['Fecha/Hora'].dt.date

max_date = df['Fecha'].max()
min_date = df['Fecha'].min()

if max_date > min_date:
    where_date = f"EXTRACT(DATE FROM TIMESTAMP_ADD(C.checkOutDate, INTERVAL -6 HOUR)) BETWEEN '{min_date}' AND '{max_date}'"
else:
    where_date = f"EXTRACT(DATE FROM TIMESTAMP_ADD(C.checkOutDate, INTERVAL -6 HOUR)) = '{max_date}'"

query_kigo = f"""
SELECT TIMESTAMP_ADD(C.checkOutDate, INTERVAL -6 HOUR) AS Salidas, L.QR, L.function_, CAT.gateName, C.userId, c.id, T.qrCode AS ticket, 
FROM `parkimovil-app`.cargomovil_pd.PKM_SMART_QR_CHECKOUT C
JOIN `parkimovil-app`.cargomovil_pd.PKM_PARKING_LOT_GATE_CAT CAT
    ON C.parkingLotId = CAT.parkingLotId
JOIN `parkimovil-app`.geosek_raspis.log L
    ON CAT.geoSekMapping = L.QR
JOIN `parkimovil-app`.geosek_raspis.raspis R
    ON L.QR = R.qr
JOIN parkimovil-app.cargomovil_pd.PKM_SMART_QR_TRANSACTIONS T
    ON C.Id = T.checkOutId
WHERE c.parkingLotId = 233 AND gateName LIKE 'Salida%' AND {where_date}
GROUP BY C.checkOutDate, L.QR, L.function_, CAT.gateName, C.userId, c.id, T.qrCode
ORDER BY Salidas ASC;
"""

df_query_kigo = client.query(query_kigo).to_dataframe()

# Garantizar formato de datetime
df_query_kigo['Salidas'] = pd.to_datetime(df_query_kigo['Salidas'])

df['KIGO'] = False

for index, row in df.iterrows():
    filtro = (df_query_kigo['gateName'] == row['Etiqueta Salida']) & (abs(df_query_kigo['Salidas'] - row['Fecha/Hora']) < pd.Timedelta(seconds=3))
    if filtro.any():
        df.at[index, 'KIGO'] = True
        ticket_info = df_query_kigo.loc[filtro]
        ticket = ticket_info['ticket'].values[0]
        salida = ticket_info['Salidas'].values[0]
        salida_kigo = ticket_info['QR'].values[0]
        df.at[index, 'Ticket'] = ticket
        df.at[index, 'Fecha/Hora Kigo'] = salida
        df.at[index, 'QR'] = salida_kigo

st.write("Análisis")
st.dataframe(df)
@st.cache_data
def get_binary_file_downloader_html(bin_file, file_label='File'):
    with open(bin_file, 'rb') as f:
        data = f.read()
    bin_str = base64.b64encode(data).decode()
    href = f'<a href="data:application/octet-stream;base64,{bin_str}" download="{bin_file}">{file_label}</a>'
    return href

if st.button('Descargar tabla como Excel'):
        with pd.ExcelWriter('kigo_arboleda.xlsx', engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False)
        st.success('Tabla descargada exitosamente!')
        st.markdown(get_binary_file_downloader_html('kigo_arboleda.xlsx', 'Descargar tabla como Excel'), unsafe_allow_html=True)
