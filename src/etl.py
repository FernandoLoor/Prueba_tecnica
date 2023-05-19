import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import pyarrow.orc as orc
import plotly.express as px


def aggregateDataframeOutcome(df,Frequency):
    
    #### Agregación por time: "H" => hourly frequency; "T, min" => minutely frequency    
    df['agg_Date_'+Frequency] = df['Date'].dt.floor(Frequency)

    grouped_data = df.groupby(['agg_Date_'+Frequency])

    aggregation_dict = {'trade_id': 'mean', 'price': 'mean', 'quantity': 'mean', 
                        'time': 'min','Hour': 'min', 'transaction_value': 'sum',
                        'outcome': 'last',  'transaction_value_abs': 'sum'}
    aggregated_data = grouped_data.agg(aggregation_dict).reset_index()

    aggregated_data['Outcome_Balance'] = np.where(aggregated_data['outcome'] >= 0, 'Positive', 'Negative')

    aggregated_data.to_csv("Aggregated_"+Frequency+"_outcome.csv",index=False)

    return aggregated_data


def aggregateDataframeTrades(df,Frequency):

    df['agg_Date_'+Frequency] = df['Date'].dt.floor(Frequency)
    
    grouped_data = df.groupby(['agg_Date_'+Frequency,"isSelling"])

    aggregation_dict = {'trade_id': 'mean', 'price': 'mean', 'quantity': 'mean', 
                    'time': 'min','Hour': 'min', 'transaction_value': 'sum',
                    'outcome': 'sum',  'transaction_value_abs': 'sum'}
    
    aggregated_data = grouped_data.agg(aggregation_dict).reset_index()

    return aggregated_data


def aggregateDataframeChains(df):
    df['chainIdx'] = df['isSelling'].groupby((df['isSelling'].diff()!=0).cumsum()).ngroup() 
    grouped_data = df.groupby(["chainIdx"])

    #Usar una agregación específica para cada columna:
    aggregation_dict = {'trade_id': 'first', 'price': 'mean', 'quantity': 'mean', 
                        'time': 'first','Date': 'first',#'Time': 'first',
                        'Hour': 'min', 'transaction_value': 'sum',
                        'outcome': 'last',  'transaction_value_abs': 'sum',  'consecutive': 'first','isSelling':'first'}

    aggregated_data = grouped_data.agg(aggregation_dict).reset_index()

    top_chains = aggregated_data.nlargest(50,"transaction_value_abs").reset_index()

    return top_chains


def etl(df):
    print("----------> ETL, df.shape:")
    print(df.shape)
    print(df.head)
    print(df)
    print(df.dtypes)
    df["quantity"]=np.float64(df["quantity"])
    df["time"]=np.float64(df["time"])
    #df["isSelling"]=df["isSelling"]=="True"
    print(df.dtypes)

    #Convertir formato fecha:
    df['Date'] = pd.to_datetime(df['time'], unit='ms')
    df['Hour'] = df['Date'].dt.hour

    #Eliminar valores inválidos.
    df=df.dropna().reset_index(drop=True).copy()            #Eliminar NaNs
    df=df[df["price"]>0].copy().reset_index(drop=True)      #Eliminar trades con precio negativo
    df=df[df["quantity"]>0].copy().reset_index(drop=True)      #Eliminar trades con cantidad negativa

    df['consecutive'] = df['isSelling'].groupby((df['isSelling'] != df['isSelling'].shift()).cumsum()).transform('size')# * df.Col2

    #Añadir columnas transaction_value y outcome. A transaction_value darle signo negativo si es Buy
    df["transaction_value"]=df["price"]*df["quantity"]
    df.loc[df["isSelling"] == False, "transaction_value"] = df.loc[df["isSelling"] == False, "transaction_value"]*(-1)
    df["transaction_value_abs"]=abs(df["transaction_value"])

    df["outcome"]=df["transaction_value"].cumsum()
    df['Outcome_Balance'] = np.where(df['outcome'] >= 0, 'Positive', 'Negative')
    
    aggregatedDataOutcome_H = aggregateDataframeOutcome(df,"H")
    aggregatedDataOutcome_T = aggregateDataframeOutcome(df,"T")
    aggregatedDataTrades_H = aggregateDataframeTrades(df,"H")
    aggregatedDataTrades_T = aggregateDataframeTrades(df,"T")
    top_chains = aggregateDataframeChains(df)
    
    df_buy = df[df["isSelling"]==False]
    df_sell = df[df["isSelling"]==True]
    total_buy_value=df_buy["transaction_value_abs"].sum()
    total_sell_value=df_sell["transaction_value_abs"].sum()
    lastOutcome=total_sell_value-total_buy_value

    longestSellChainLength = df[df["isSelling"]==True]["consecutive"].max() 

    df2 = df[df["isSelling"]==True ].copy()
    df2 = df2[df2["consecutive"]==longestSellChainLength]
    firstPricelongestSellChain = df2.iloc[0,:]["price"]
    lastPricelongestSellChain = df2.iloc[-1,:]["price"]
    Date = df2.iloc[0,:]["Date"]

    print("ETL Date")
    print(Date)
    data = [lastOutcome, longestSellChainLength, firstPricelongestSellChain, lastPricelongestSellChain, Date]
    print("ETL data")
    print(data)

    #return [aggregated_data_H, aggregated_data_T, top_chains, data]
    return [aggregatedDataOutcome_H, aggregatedDataOutcome_T, aggregatedDataTrades_H, aggregatedDataTrades_T, top_chains, data]
