import streamlit as st
import pandas as pd
from sqlalchemy import create_engine

st.set_page_config(
    page_title="Brand Health Tracker",
    page_icon="🎵",
)

st.title('Brand Health Tracker')
st.subheader('Musical Instruments')

@st.cache_resource
def init_connection():

    return create_engine("mssql+pyodbc:///?odbc_connect="
        + "DRIVER={ODBC Driver 17 for SQL Server};SERVER="
        + st.secrets["server"]
        + ";DATABASE="
        + st.secrets["database"]
        + ";UID="
        + st.secrets["username"]
        + ";PWD="
        + st.secrets["password"]
    )

conn = init_connection()

@st.cache_data
def get_top_10_brands():
    df = pd.read_sql("""
        SELECT TOP 10 m.brand, COUNT(r.asin) AS review_count
        FROM metadata m
        INNER JOIN reviews r ON m.asin = r.asin
        WHERE m.brand <> ''
        GROUP BY m.brand
        ORDER BY review_count DESC;
    """, conn)

    return df['brand'].to_list()

brands = get_top_10_brands()

brand1 = st.selectbox("Select first brand", options=brands)
brand2 = st.selectbox("Select second brand", options=brands, index=1)
