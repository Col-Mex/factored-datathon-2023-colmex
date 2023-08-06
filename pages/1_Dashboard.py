import streamlit as st
import datetime
import numpy as np
import plotly.graph_objects as go
from dateutil.relativedelta import relativedelta
import pandas as pd
# from streamlit_extras.add_vertical_space import add_vertical_space

st.set_page_config(page_title="Brand Health Tracker", page_icon="📈")

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


row0_spacer1, row0_1, row0_spacer2, row0_2, row0_spacer3, row0_3, row0_spacer4 = st.columns(
    (0.05, 1, 0.1, 1, 0.1, 1, 0.05)
)

row0_1.subheader("Select the dates for the analysis:")

with row0_2:
    d_ini = st.date_input("Beginning date", datetime.date(2022, 7, 27))

with row0_2:
    d_fin = st.date_input("Ending date", datetime.date(2023, 7, 27))

st.write(d_ini, d_fin)

@st.cache_data
def date_filter(ini_date, end_date):
    df = pd.read_sql(f"""
        SELECT r.dateReview, m.brand, r.reviewText, r.summary, r.sentiment, r.emotion
        FROM reviews r
        LEFT JOIN metadata m ON r.asin = m.asin
        WHERE m.brand <> '' AND r.dateReview >= '{str(ini_date)}' AND r.dateReview <= '{str(end_date)}'
        ORDER BY r.dateReview ASC;
    """, conn)

    return df

data = date_filter(d_ini, d_fin)
st.subheader('Raw data')
st.write(data.head())
