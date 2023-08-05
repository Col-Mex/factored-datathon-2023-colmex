import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from dateutil.relativedelta import relativedelta

st.set_page_config(
    page_title="Hello",
    page_icon="👋",
)

st.title('ColMex App')

DATA_URL = ('data_files/data_test_streamlit.parquet')

@st.cache_data
def load_data():
    data = pd.read_parquet(DATA_URL)
    # Rescaled overall score
    data.loc[:, 'overall_health'] = data['overall'] - 3
    # Rescaled sentiment score
    data.loc[:, 'sentiment_health'] = np.where(data['sentiment']=='positive', 2, 
            np.where(data['sentiment']=='negative', -2, 0))
    # Average of both measures: the overall tracker
    data.loc[:, 'tracker_health'] = (data['overall_health'] + data['sentiment_health']) / 2
    return data

data = load_data()
brands_list = list(data['brand'].unique())

st.subheader('Raw data')
if st.checkbox('Show raw data'):
    st.subheader('Raw data')
    st.write(data.head())