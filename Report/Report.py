#!pip install dash
import dash, html
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import plotly.express as px
import pandas as pd
from datetime import datetime, date, timedelta

from etl import *

import sys
import os
path = os.path.abspath("../DB-client-server-REST/")
sys.path.append(path)
from getElements import *


external_stylesheets = [
    "https://fonts.googleapis.com/css2?family=Press+Start+2P&display=swap",
    'https://fonts.googleapis.com/css2?family=Roboto:wght@300&display=swap',
    "https://fonts.googleapis.com/css2?family=Poppins&display=swap"
]


# Create the Dash app
app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

#aggregated_outcome_hour=pd.read_csv("Aggregated_H_outcome.csv")#,index=False),
#aggregated_outcome_minute=pd.read_csv("Aggregated_T_outcome.csv")#,index=False),
#aggregated_trades_hour=pd.read_csv("Aggregated_H_trades.csv")#,index=False),
#aggregated_trades_minute=pd.read_csv("Aggregated_T_trades.csv")#,index=False),

date1='2023-03-31T00:00:00'
date2='2023-03-31T23:59:59'

df_day = getElements(date1,date2)

[aggregatedDataOutcome_H, aggregatedDataOutcome_T, aggregatedDataTrades_H, aggregatedDataTrades_T, top_chains, data] = etl(df_day)

# Define the layout of the app
app.layout = html.Div([
    html.H1('Trading bot stats report',
        style={"font-family": "Poppins"}              #Este nombre debe matchear el nombre de la fuente que se importa más arriba, en external sheets
    ),
    
    html.Div(
            children=[
            #html.H3("ID of biggest trade of the day: "+ID_biggest_trade),
            #html.H3("Price of biggest trade of the day: "+Price_biggest_trade),
            #html.H3("Quantity of biggest trade of the day: "+Quantity_biggest_trade),
            #html.H3("Value of biggest trade of the day: "+value_biggest_trade),
            #html.H3("Longest chain of consecutive buys: " + max_consecutive_buys),
            html.H3("Bot's outcome for the day: "+ str(data[0])),
            html.H3("Size of longest chain of consecutive sells: " + str(data[1])),
            html.H3("Difference between prices of the first and last operation of that chain: "+ str(data[3] - data[2]) ),
            ],
            style={'width': '40%', 'display': 'inline-block',"font-family": "Poppins"}
        ),    
    html.Div(
            children=[
            html.H3("Select date"),
            dcc.DatePickerSingle(
                id='date-picker',
                #min_date_allowed=dates.min(),
                #max_date_allowed=dates.max(),
                initial_visible_month=date1,
                date=date1,
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
                        color_discrete_sequence=px.colors.qualitative.Set1
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
            Output('Bot trades accumulated by minute','figure')],
              [Input('date-picker', 'date'),
              Input('interval-component', 'n_intervals')])
def update_graph(selected_date,n):
    date1 = pd.to_datetime(selected_date)
    print("date1")
    print(date1)
    date2 = date1 + timedelta(days=1)
    print("date2")
    print(date2)

    df_day = getElements(date1,date2)

    [aggregatedDataOutcome_H, aggregatedDataOutcome_T, aggregatedDataTrades_H, aggregatedDataTrades_T, top_chains, data] = etl(df_day)
    print(aggregatedDataOutcome_H)
    fig1 = px.bar(aggregatedDataOutcome_H, x='agg_Date_H', y="outcome", color="Outcome_Balance",
                        color_discrete_sequence=px.colors.qualitative.Set1
                    )
    fig2 = px.bar(aggregatedDataTrades_H, x='agg_Date_H', y="transaction_value_abs", color="isSelling",
                     barmode='group',color_discrete_sequence=px.colors.qualitative.Set2
                    )
    fig3 = px.bar(aggregatedDataOutcome_H, x='agg_Date_H', y="price",
                color_discrete_sequence=["dark grey"]
                ).update_layout(yaxis_range=[aggregatedDataOutcome_H["price"].min()*0.99,aggregatedDataOutcome_H["price"].max()*1.01])
    fig4 = px.scatter(aggregatedDataTrades_T, x='agg_Date_T', y="transaction_value_abs", 
                     color="isSelling",size='transaction_value_abs',
                     color_discrete_sequence=px.colors.qualitative.Set2
                    )
    return [fig1,fig2,fig3,fig4]

# Run the app
if __name__ == '__main__':
    app.run_server(debug=True)