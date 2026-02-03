import os
from dotenv import load_dotenv
from confluent_kafka import Producer
import yfinance as yf
import requests


load_dotenv()

p = Producer(
    {
        "bootstrap.servers": os.getenv("BOOTSTRAP.SERVERS"),
        "security.protocol": os.getenv("SECURITY.PROTOCOL"),
        "sasl.mechanisms": os.getenv("SASL.MECHANISMS"),
        "sasl.username": os.getenv("SASL.USERNAME"),
        "sasl.password": os.getenv("SASL.PASSWORD"),
    }
)


def delivery_report(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print("Message delivery failed: {}".format(err))
    else:
        print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))

    return "DONE."

companies = ["ADBE",
             "ADP",
             "AMD",
             "ADI",
             "ANSS",
             "AAPL",
             "AMAT",
             "ASML",
             "TEAM",
             "ADSK",
             "AVGO",
             "CDNS",
             "CSCO",
             "CTSH",
             "CRWD",
             "DDOG",
             "DOCU",
             "FISV",
             "FTNT",
             "INTC",
             "INTU",
             "KLAC",
             "MRVL",
             "MCHP",
             "MU",
             "MSFT",
             "NVDA",
             "NXPI",
             "OKTA",
             "PANW",
             "PAYX",
             "PYPL",
             "QCOM",
             "SWKS",
             "SPLK",
             "SNPS",
             "TXN",
             "VRSN",
             "WDAY",
             "ZM",
             "ZS",
             "ATVI",
             "GOOGL",
             "GOOG",
             "BIDU",
             "CHTR",
             "CMCSA",
             "EA",
             "MTCH",
             "META",
             "NTES",
             "NFLX",
             "SIRI"]


def initialize_yf_tickers(companies: list):
    # Initialize yahooFinance Ticker for multiple companies and return it
    ticker = yf.Tickers(" ".join(companies))
    return ticker


# Crawling web page with given URL
# param :yahoo_finance: To crawl data from yahooFinance, we have to send a User-Agent Header, otherwise YF will block the request
def get(url, yahoo_finance=False):
    yahoo_finance_header = {
        "User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:89.0) Gecko/20100101 Firefox/89.0"
    }
    if yahoo_finance:
        res = requests.get(url, headers=yahoo_finance_header)
    else:
        res = requests.get(url)
    if res.status_code == 200:
        return res.text.strip()
    else:
        raise Exception


