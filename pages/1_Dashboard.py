import streamlit as st
import datetime
import numpy as np
import plotly.graph_objects as go
from dateutil.relativedelta import relativedelta
import pandas as pd
from sqlalchemy import create_engine
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
    d_ini = st.date_input("Starting date", datetime.date(2022, 7, 27))

with row0_3:
    d_fin = st.date_input("Ending date", datetime.date(2023, 7, 25))

row1_spacer1, row1_1, row1_spacer2, row1_2, row1_spacer3 = st.columns(
    (0.05, 1.5, 0.1, 1.7, 0.05)
)

row1_1.subheader("Select the brands to compare:")

with row1_2:
    brands_list = st.multiselect(
    'Brands: ',
    ["Pyle", "Hosa", "Shure", "Behringer", "Yamaha", "Fender", 
     "Audio-Technica", "ION Audio", "DragonPad USA"], ['Pyle', 'Yamaha', 'Audio-Technica'])

@st.cache_data
def date_filter(ini_date, end_date):
    df = pd.read_sql(f"""
        SELECT r.dateReview, m.brand, r.reviewText, r.summary, r.sentiment, r.emotion, r.overall
        FROM reviews r
        LEFT JOIN metadata m ON r.asin = m.asin
        WHERE m.brand <> '' AND r.dateReview >= '{str(ini_date)}' AND r.dateReview <= '{str(end_date)}'
        ORDER BY r.dateReview ASC;
    """, conn)
    # Rescaled overall score
    df.loc[:, 'scores_health'] = df['overall'] - 3
    # Rescaled sentiment score
    df.loc[:, 'sentiment_health'] = np.where(df['sentiment']=='positive', 2, 
            np.where(df['sentiment']=='negative', -2, 0))
    # Average of both measures: the overall tracker
    df.loc[:, 'combined'] = (df['scores_health'] + df['sentiment_health']) / 2

    return df

def cumulative_reviews(data, brands_included):
    data.sort_values(by='dateReview', inplace=True)
    # Create the figure
    fig = go.Figure()
    cumulative_reviews = {}
    for brand in brands_included:
        cumulative_reviews[brand] = (data['brand'] == brand).cumsum()
        # Add traces
        fig.add_trace(go.Scatter(x=data['dateReview'], y=cumulative_reviews[brand],
                            mode='lines',
                            name=brand))

    # Set title and axis labels
    fig.update_layout(title='Cumulative reviews over time',
                    xaxis_title='Date',
                    yaxis_title='Number of Reviews',
                    hovermode='x unified')
    return fig

def health_tracker_graphs(df, brands_included, indicator='tracker_health', scale='relative'):
    df_copy = df.copy().set_index('dateReview').sort_index()

    # Get the date range of the data to graph all series on the same x-axis
    date_range = pd.date_range(start=df_copy.index.min(), end=df_copy.index.max())

    # Create the plotly figure
    fig = go.Figure()

    # Calculate indicators for each brand ['Yamaha', 'Fender']
    # Create a dict that stores the data for each brand
    data_dict = {}
    error_text = ''
    for name_brand in brands_included:
        data_dict[name_brand] = df_copy.loc[df_copy['brand']==name_brand, indicator]
        if data_dict[name_brand].shape[0] < 5:
            error_text = f"Plot can't be show correctly <br> as {name_brand} has less than 5 reviews <br> on the selected span of time. <br> Please, increase the period length"
            break

        if scale == 'absolute':
            data_dict[name_brand] = data_dict[name_brand].groupby(data_dict[name_brand].index).sum().cumsum().asfreq('D', method='ffill')
        elif scale == 'relative':
            data_dict[name_brand] = data_dict[name_brand].groupby(data_dict[name_brand].index).agg(['sum', 'count']).cumsum()
            data_dict[name_brand] = (data_dict[name_brand]['sum'] / data_dict[name_brand]['count']).asfreq('D', method='ffill')
        else:
            raise ValueError("Invalid value for parameter 'scale'. Expected either 'absolute' or 'relative'")
        
        # Reindex series for brand, so it starts and ends in the same date
        data_dict[name_brand] = data_dict[name_brand].reindex(date_range, fill_value=data_dict[name_brand].iloc[-1])
        # health_yamaha = health_yamaha.reindex(date_range, fill_value=health_yamaha.iloc[-1])
        # Add line of data
        fig.add_trace(go.Scatter(x=date_range, y=data_dict[name_brand],
                        mode='lines',
                        name=name_brand))

    # Set title and axis labels
    fig.update_layout(title=f'{scale} {indicator} over time',
                    xaxis_title='Date',
                    yaxis_title='Score',
                    hovermode='x unified',
                    annotations=[dict(text=error_text, showarrow=False, xref='paper', yref='paper',
                                      x=0, y=1, xanchor='left', yanchor='top', font=dict(size=40, color='black'))]
                                      )
    return fig

data = date_filter(d_ini, d_fin)

st.subheader('Data exploration')
tab1, tab2, tab3 = st.tabs(["Cumulative number of reviews", "Reviews by sentiment", "Reviews by emotion"])
with tab1:
    fig = cumulative_reviews(data, brands_list)
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
with tab2:
    st.write('r')
with tab3:
    st.write('r')

st.subheader('Brand Health Tracker - ColMex :tm:')
tab1, tab2, tab3 = st.tabs(["Combined - Scores and sentiment", "Tracker of review sentiment", "Tracker of review score"])
with tab1:
    fig1 = health_tracker_graphs(data, brands_list,
                                 indicator='combined', scale='relative')
    st.plotly_chart(fig1, theme="streamlit", use_container_width=True)
with tab2:
    fig2 = health_tracker_graphs(data, brands_list,
                                 indicator='sentiment_health', scale='relative')
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)
with tab3:
    fig3 = health_tracker_graphs(data, brands_list,
                                 indicator='scores_health', scale='relative')
    st.plotly_chart(fig3, theme="streamlit", use_container_width=True)
