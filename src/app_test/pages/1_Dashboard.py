
import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
from dateutil.relativedelta import relativedelta

st.set_page_config(page_title="Dashboard", page_icon="📈")

st.title('ColMex App')

DATA_URL = ('data/data.parquet')

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

def cumulative_reviews(data):
    data.sort_values(by='dateReview', inplace=True)
    # Create the figure
    fig = go.Figure()
    cumulative_reviews = {}
    for brand in brands_list:
        cumulative_reviews[brand] = (data['brand'] == brand).cumsum()
        # Add traces
        fig.add_trace(go.Scatter(x=data['dateReview'], y=cumulative_reviews[brand],
                            mode='lines',
                            name=brand))

    # Set title and axis labels
    fig.update_layout(title='Cumulative Reviews Over Time',
                    xaxis_title='Date',
                    yaxis_title='Number of Reviews',
                    hovermode='x unified')
    return fig

def health_tracker_graphs(df, brands_included, period='months', length=1, indicator='tracker_health', scale='relative'):
    # Calculate init date based on 'period' and 'lenght' parameters
    init_date = df['dateReview'].max() - relativedelta(**{period: length})
    df_copy = df.copy().set_index('dateReview').sort_index()
    
    # Select only the rows that fall on the desired time laps
    df_copy = df_copy[df_copy.index >= init_date]

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
    fig.update_layout(title=f'{scale} {indicator} over the last {length} {period}',
                    xaxis_title='Date',
                    yaxis_title='Score',
                    hovermode='x unified',
                    annotations=[dict(text=error_text, showarrow=False, xref='paper', yref='paper',
                                      x=0, y=1, xanchor='left', yanchor='top', font=dict(size=40, color='black'))]
                                      )
    return fig

data = load_data()
brands_list = list(data['brand'].unique())

st.subheader('Graphs')
tab1, tab2 = st.tabs(["Cumulative number of reviews", "Tab2"])
with tab1:
    fig = cumulative_reviews(data)
    st.plotly_chart(fig, theme="streamlit", use_container_width=True)
with tab2:
    brands_list_plot = ['Audio-Technica', 'Pyle']
    period='days'
    length=85
    indicator='tracker_health'
    scale='relative'
    fig2 = health_tracker_graphs(data, brands_list_plot,
                                 period=period, length=length,
                                 indicator=indicator, scale=scale)
    st.plotly_chart(fig2, theme="streamlit", use_container_width=True)
