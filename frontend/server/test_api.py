import pandas as pd
import pymongo
import certifi
from flask import Flask
from flask_restx import Resource, Api
from datetime import datetime, timedelta, date
import re
import json
from io import StringIO
from ast import literal_eval
import requests
from pyrsistent import v
import plotly as plt
import plotly.graph_objects as go
symbol = "MSFT"

request_history_price = requests.get(f"http://127.0.0.1:5000/stock_price_history/{symbol}").json()

history_df = pd.DataFrame.from_dict(literal_eval(request_history_price))
history_df.index = pd.to_datetime(history_df.index, unit="ms") + timedelta(hours=2)

def give_candlestick_chart(df: pd.DataFrame):
    candlestick_chart = go.Figure(
        data=[
            go.Candlestick(
                x=df.index,
                open=df["Open"],
                high=df["High"],
                low=df["Low"],
                close=df["Close"],
            )
        ]
    )

    candlestick_chart.update_xaxes(
        rangeslider_visible=True,
        rangeselector=dict(
            buttons=list(
                [
                    dict(step="all"),
                ]
            )
        ),
    )
    return candlestick_chart.show()

if __name__ == "__main__":
    give_candlestick_chart(history_df)
