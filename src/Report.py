#!pip install dash
import dash, html
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from datetime import datetime, date, timedelta
import sys
import os

#from etl import *
#path = os.path.abspath("../DB-client-server-REST/")
#sys.path.append(path)
#from getElements import *


def loadDataframes(date1,date2):

    aggregatedDataOutcome_H=pd.read_orc("../AggregatedDatasets/OutcomesH.orc")
    aggregatedDataOutcome_T=pd.read_orc("../AggregatedDatasets/OutcomesT.orc")
    aggregatedDataTrades_H=pd.read_orc("../AggregatedDatasets/TradesH.orc")
    aggregatedDataTrades_T=pd.read_orc("../AggregatedDatasets/TradesT.orc")
    top_chains=pd.read_orc("../AggregatedDatasets/TopChains.orc")
    data=pd.read_orc("../AggregatedDatasets/Data.orc")

    maxDate=aggregatedDataOutcome_H['agg_Date_H'].max()
    minDate=aggregatedDataOutcome_H['agg_Date_H'].min()

    date1 = pd.to_datetime(date1, format="mixed")

    if date1 < minDate:
        date1 = minDate
        date2 = date1 + timedelta(days=1)
        date2 = date2.floor(freq='D')
        print("Dates below range")

    if date1 > maxDate:
        date2 = maxDate
        date1 = date2 - timedelta(days=1)
        date1 = date1.floor(freq='D')
        print("Dates above range")

    aggregatedDataOutcome_H=aggregatedDataOutcome_H.query('agg_Date_H >= @date1 & agg_Date_H < @date2')
    aggregatedDataOutcome_T=aggregatedDataOutcome_T.query('agg_Date_T >= @date1 & agg_Date_T < @date2')
    aggregatedDataTrades_H=aggregatedDataTrades_H.query('agg_Date_H >= @date1 & agg_Date_H < @date2')
    aggregatedDataTrades_T=aggregatedDataTrades_T.query('agg_Date_T >= @date1 & agg_Date_T < @date2')
    top_chains=top_chains.query('Date >= @date1 & Date < @date2')
    data=data.query('Date >= @date1 & Date < @date2')

    return [aggregatedDataOutcome_H, aggregatedDataOutcome_T, aggregatedDataTrades_H, aggregatedDataTrades_T, top_chains, data, minDate, maxDate - timedelta(days=1)] 


external_stylesheets = [
    "https://fonts.googleapis.com/css2?family=Press+Start+2P&display=swap",
    'https://fonts.googleapis.com/css2?family=Roboto:wght@300&display=swap',
    "https://fonts.googleapis.com/css2?family=Poppins&display=swap"
]


# Create the Dash app
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

date1='2023-03-31T00:00:00'
date2='2023-03-31T23:59:59'

[aggregatedDataOutcome_H, aggregatedDataOutcome_T, aggregatedDataTrades_H, aggregatedDataTrades_T, top_chains, data, minDate,maxDate] = loadDataframes(date1,date2)
date2=maxDate
date1=date2 - timedelta(days=1)

# Define the layout of the app
app.layout = html.Div([
    html.H1('Trading bot stats report',
        style={"font-family": "Poppins"}              #Este nombre debe matchear el nombre de la fuente que se importa más arriba, en external sheets
    ),
    
    html.Div(
            children=[
            html.H3("Bot's outcome for the day: "+ str(data["lastOutcome"].values)),
            html.H3("Size of longest chain of consecutive sells: " + str(data["longestSellChainLength"].values)),
            html.H3("Difference between prices of the first and last operation of that chain: "+ str(data["lastPricelongestSellChain"].values - data["firstPricelongestSellChain"].values) ),
            ],
            style={'width': '40%', 'display': 'inline-block',"font-family": "Poppins"}
        ),    
    html.Div(
            children=[
            html.H3("Select date"),
            dcc.DatePickerSingle(
                id='date-picker',
                min_date_allowed=minDate,
                max_date_allowed=maxDate ,
                initial_visible_month=minDate,
                date=minDate,
            ),
            ],
            style={'width': '40%', 'display': 'inline-block',"font-family": "Poppins"}
        ),

    html.Div([
        html.Div(
             children=[
                html.H2('Bot accumulated outcome by hour'),
                dcc.Graph(
                    id='Bot accumulated outcome by hour',
                    figure = px.bar(aggregatedDataOutcome_H, x='agg_Date_H', y="outcome", color="Outcome_Balance",
                        color_discrete_map={'Positive':px.colors.qualitative.Set1[1], 'Negative':px.colors.qualitative.Set1[0]}
                    )
                ),
                dcc.Interval(
                    id='interval-component',
                    interval=10*1000, # in milliseconds
                    n_intervals=0
                )
            ]
             ,style={'width': '40%', 'display': 'inline-block',"font-family": "Poppins"},id='Div1'
        , className='row'),

        html.Div(
             children=[
                html.H2('Bot trades accumulated by hour'),
                dcc.Graph(
                    id='Bot trades accumulated by hour',
                    figure = px.bar(aggregatedDataTrades_H, x='agg_Date_H', y="transaction_value_abs", color="isSelling",
                        barmode='group',color_discrete_sequence=px.colors.qualitative.Set2
                    )
                ),
            ]
            ,style={'width': '40%', 'display': 'inline-block',"font-family": "Poppins"}
        , className='row'),

        html.Div(
             children=[
                html.H2('Hourly price variation'),
                dcc.Graph(
                    id='Hourly price variation',
                    figure = px.bar(aggregatedDataOutcome_H, x='agg_Date_H', y="price",
                    color_discrete_sequence=["dark grey"]
                    ).update_layout(yaxis_range=[aggregatedDataOutcome_H["price"].min()*0.99,aggregatedDataOutcome_H["price"].max()*1.01])
                )
            ]
             ,style={'width': '40%', 'display': 'inline-block',"font-family": "Poppins"}
        , className='row'),

        html.Div(
             children=[
                html.H2('Bot trades accumulated by minute'),
                dcc.Graph(
                    id='Bot trades accumulated by minute',
                    figure = px.scatter(aggregatedDataTrades_T, x='agg_Date_T', y="transaction_value_abs", 
                     color="isSelling",size='transaction_value_abs', 
                     color_discrete_sequence=px.colors.qualitative.Set2
                    )
                )
            ]
            ,style={'width': '40%', 'display': 'inline-block',"font-family": "Poppins"}
        , className='row'),

    ]),
    

], className='container')

@app.callback(
            [Output('Bot accumulated outcome by hour','figure'),
            Output('Bot trades accumulated by hour','figure'),
            Output('Hourly price variation','figure'),
            Output('Bot trades accumulated by minute','figure'),
            Output('date-picker','min_date_allowed'),
            Output('date-picker','max_date_allowed')],
              [Input('date-picker', 'date'),
              Input('interval-component', 'n_intervals')])
def update_graph(selected_date,n):
    
    date1 = pd.to_datetime(selected_date).floor(freq='D')
    date2 = date1 + timedelta(days=1)
    date2 = date2.floor(freq='D')

    [aggregatedDataOutcome_H, aggregatedDataOutcome_T, aggregatedDataTrades_H, aggregatedDataTrades_T, top_chains, data, minDate, maxDate] = loadDataframes(date1,date2)
    
    fig1 = px.bar(aggregatedDataOutcome_H, x='agg_Date_H', y="outcome", color="Outcome_Balance",
                        color_discrete_map={'Positive':px.colors.qualitative.Set1[1], 'Negative':px.colors.qualitative.Set1[0]}
                    )
    #fig1.write_html("fig1.html")

    fig2 = px.bar(aggregatedDataTrades_H, x='agg_Date_H', y="transaction_value_abs", color="isSelling",
                     barmode='group',color_discrete_sequence=px.colors.qualitative.Set2
                    )
    #fig2.write_html("fig2.html")

    fig3 = px.bar(aggregatedDataOutcome_H, x='agg_Date_H', y="price",
                color_discrete_sequence=["dark grey"]
                ).update_layout(yaxis_range=[aggregatedDataOutcome_H["price"].min()*0.99,aggregatedDataOutcome_H["price"].max()*1.01])
    #fig3.write_html("fig3.html")

    fig4 = px.scatter(aggregatedDataTrades_T, x='agg_Date_T', y="transaction_value_abs", 
                     color="isSelling",size='transaction_value_abs',
                     color_discrete_sequence=px.colors.qualitative.Set2
                    )
    #fig4.write_html("fig4.html")

    return [fig1,fig2,fig3,fig4,minDate,maxDate]


# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)