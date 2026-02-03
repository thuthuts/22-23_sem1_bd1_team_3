import pandas as pd
import pymongo
import os

from dotenv import load_dotenv  # Not neccessary for GCP AE
import certifi
import pprint as pp
from flask import Flask
from flask_restx import Resource, Api
import json
from datetime import datetime, timedelta
from ast import literal_eval
import re


# Building a restful API with flask-restx
app = Flask(__name__)
api = Api(app)


load_dotenv()  # Not neccessary for GCP AE

client = pymongo.MongoClient(
    f"mongodb+srv://{os.getenv('MONGODB_USERNAME')}:{os.getenv('MONGODB_PASSWORD')}@{os.getenv('MONGODB_URI')}/",
    tlsCAFile=certifi.where(),
)

# Databases and Collection Setup
db_esg = client["KPIs"]["esg"]
db_total_revenue = client["KPIs"]["revenue"]
db_total_operating_expenses = client["KPIs"]["total_expenses"]
db_net_income = client["KPIs"]["net_income"]
db_gross_profit = client["KPIs"]["gross_profit"]
db_ebit = client["KPIs"]["ebit"]

db_history_stock_price = client["Stocks"]["history_stock_price"]
db_stock_price_lasthour = client["Stocks"]["stock_price_lasthour"]

db_company_news = client["News"]["company_news"]
db_nasdaq_news = client["News"]["nasdaq_news"]
db_twitter_news = client["News"]["twitter_news"]
db_nasdaq_news_sm = client["News"]["ML_news"]
db_twitter_news_sm = client["News"]["twitter_news_docker"]




# Get ESG Score for a company
class ESG(Resource):
    def get(self, symbol):
        return db_esg.find_one({"company": symbol})["esg_score"]


# Get Total Revenue for a company
class TotalRevenue(Resource):
    def get(self, symbol):
        result = db_total_revenue.find_one({"company": symbol})["Total Revenue"]
        # return pd.DataFrame(result, index=["Total Revenue"]).T
        return result


# Get Total Operating Expenses for a company
class TotalOperatingExpenses(Resource):
    def get(self, symbol):
        result = db_total_operating_expenses.find_one({"company": symbol})[
            "Total Expenses"
        ]
        # return pd.DataFrame(result, index=["Total Operating Expenses"]).T
        return result


# Get Net Income for a company
class NetIncome(Resource):
    def get(self, symbol):
        result = db_net_income.find_one({"company": symbol})["Net Income"]
        return result


# Get Gross Profit for a company
class GrossProfit(Resource):
    def get(self, symbol):
        result = db_gross_profit.find_one({"company": symbol})["Gross Profit"]
        return result


# Get EBIT for a company
class EBIT(Resource):
    def get(self, symbol):
        result = db_ebit.find_one({"company": symbol})["EBIT"]
        return result

class HistoryStockPrice(Resource):
    def get(self, symbol):
        return db_history_stock_price.find_one({"company": symbol})[
            "history_stock_price"
        ]


class StockPriceOverPeriod(Resource):
    def get(self, symbol, date, time):
        result = list()
        time_string = date + " " + time
        query = db_stock_price_lasthour.find(
            {"time": {"$lt": time_string}, "company": symbol}
        )
        for data in query:
            del data["_id"]
            result.append(data)
        return result


class CompanyNews(Resource):
    def get(self, company, date, time):
        cursor = (
            db_company_news.find(
                {"company": company, "time": {"$lte": date + " " + time + ":59"}}
            )
            .limit(25)
            .sort([("$natural", -1)])
        )
        result = []
        for obj in cursor:
            del obj["_id"]
            result.append(obj)
        return result


class CompanyNews24h(Resource):
    def get(self, company, date, time):
        today_strtime = date + " " + time + ":59"
        today_date = datetime.strptime(today_strtime, "%Y-%m-%d %H:%M:%S")
        yesterday_date = today_date - timedelta(days=1.0, minutes=1.0)
        yesterday_strtime = datetime.strftime(yesterday_date, "%Y-%m-%d %H:%M:%S")
        cursor = db_company_news.find(
            {
                "company": company,
                "$and": [
                    {"timestamp": {"$lte": today_strtime}},
                    {"timestamp": {"$gte": yesterday_strtime}},
                ],
            }
        ).sort([("$natural", -1)])
        result = []
        for obj in cursor:
            del obj["_id"]
            result.append(obj)
        return result


class NasdaqNews(Resource):
    def get(self, date, time):
        cursor = (
            db_nasdaq_news.find({"time": {"$lte": date + " " + time + ":59"}})
            .limit(25)
            .sort([("$natural", -1)])
        )
        result = []
        for obj in cursor:
            del obj["_id"]
            result.append(obj)
        return result

class NasdaqNewsSM(Resource):
    def get(self, date, time):
        cursor = (
            db_nasdaq_news_sm.find({"timestamp": {"$lte": date + " " + time + ":59"}})
            .limit(25)
            .sort([("$natural", -1)])
        )
        result = []
        for obj in cursor:
            del obj["_id"]
            result.append(obj)
        return result

class TwitterNews(Resource):
    def get(self, company, date, time):
        cursor = (
            db_twitter_news.find(
                {"company": company, "time": {"$lte": date + " " + time + ":59"}}
            )
            .limit(25)
            .sort([("$natural", -1)])
        )
        result = []
        for obj in cursor:
            del obj["_id"]
            result.append(obj)
        return result

class TwitterNewsSM(Resource):
    def get(self, company, date, time):
        cursor = (
            db_twitter_news_sm.find(
                {"company": company, "time": {"$lte": date + " " + time + ":59"}}
            )
            .limit(25)
            .sort([("$natural", -1)])
        )
        result = []
        for obj in cursor:
            del obj["_id"]
            result.append(obj)
        return result


# Add our API Endpoints
# Dashboard: Key Performance Indicators
# Status: No problems, all working
api.add_resource(ESG, "/esg_score/<symbol>")
api.add_resource(TotalRevenue, "/total_revenue/<symbol>")
api.add_resource(TotalOperatingExpenses, "/total_operating_expenses/<symbol>")
api.add_resource(NetIncome, "/net_income/<symbol>")
api.add_resource(GrossProfit, "/gross_profit/<symbol>")
api.add_resource(EBIT, "/ebit/<symbol>")


# Dashboard: Investor Relations
# Status: No problems, all working
api.add_resource(HistoryStockPrice, "/stock_price_history/<symbol>")
api.add_resource(StockPriceOverPeriod, "/stock_price/<symbol>/<date>/<time>")


# Dashboard: Company Environment
# Status: No problems, all working
api.add_resource(CompanyNews, "/company_news_classified/<company>/<date>/<time>")
api.add_resource(CompanyNews24h, "/company_news_classified_24h/<company>/<date>/<time>")
api.add_resource(NasdaqNews, "/nasdaq_news/<date>/<time>")
api.add_resource(NasdaqNewsSM, "/nasdaq_news_sm/<date>/<time>")
api.add_resource(TwitterNews, "/twitter_news/<company>/<date>/<time>")
api.add_resource(TwitterNewsSM, "/twitter_news_sm//<company>/<date>/<time>")


if __name__ == "__main__":
    # test if you get the data
    app.run(debug=False)
