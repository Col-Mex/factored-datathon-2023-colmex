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

    return df

def get_top_10_brands_last_30_days():
    df = pd.read_sql("""
        SELECT TOP 10 m.brand, COUNT(r.asin) AS review_count
        FROM metadata m
        INNER JOIN reviews r ON m.asin = r.asin
        WHERE r.dateReview >= DATEADD(day, -30, GETDATE())
        GROUP BY m.brand
        ORDER BY review_count DESC;
    """, conn)

    return df

# df_brands = get_top_10_brands()
# brands = df_brands['brand'].to_list()

# brand1 = st.selectbox("Select first brand", options=brands)
# brand2 = st.selectbox("Select second brand", options=brands, index=1)

df_30days = get_top_10_brands_last_30_days()

st.dataframe(
    df_30days,
    column_config={
        "brand": "Brand", 
        "review_count": "Last reviews (30 days)"
    },
    hide_index=True,
)
