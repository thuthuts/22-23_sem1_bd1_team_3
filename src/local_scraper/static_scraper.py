"""
Here we will extract data from the internet which won't change in 2022
"""
from re import M
import time
import datetime as dt
import json
from bs4 import BeautifulSoup as bs
import yfinance as yf
from producersetup import (
    p,
    get,
    delivery_report,
    companies,
)


def get_history_stock_price(
    companies: list = companies + ["^NDXT"],
):
    for company in companies:
        try:
            record_value = yf.download(
                f"{company}",
                start="2021-05-01",
                end="2022-11-21",
                interval="1d",
                group_by="ticker",
            ).to_json()

            # Store company as key and stock history as value in a dict and transform it into JSON
            # Note: record_value is a DataFrame to json. In the frontend, we'll need to transform it back into a DF.
            # Steps: res = json.loads(value), then result = pd.json_normalize(res)
            p.produce(
                "history_stock_price",
                json.dumps(
                    {
                        "company": company,
                        "history_stock_price": record_value,
                        "time": str(dt.datetime.now()),
                    }
                ),
                callback=delivery_report,
            )
            p.flush()

        except Exception as e:
            # Send NaN-Value to history_stock_price topic
            message = json.dumps(
                {
                    "company": company,
                    "history_stock_price": "NaN",
                    "time": str(dt.datetime.now()),
                }
            )
            p.produce("history_stock_price", message, callback=delivery_report)
            p.flush()

            # Send Error-Object to error topic (DLQ)
            error_message = json.dumps(
                {
                    "scraper": "history_stock_price",
                    "company": company,
                    "timestamp": str(dt.datetime.now()),
                    "error": repr(e),
                    "error_type": str(type(e)),
                }
            )
            p.produce("error", error_message, callback=delivery_report)
            p.flush()
            print(f"FAILED. For {company} the following error occured: {type(e)}")

    return "Done. Produced all stock data to Kafka."

def get_ESG_score(companies: list = companies):
    # Iterate over every company and extract ESG Score
    for company in companies:
        try:
            company_ticker = yf.Ticker(f"{company}")
            # To create the value in a suitable way, we have to transpose the sustainability data frame
            # in order to extract the Total ESG Score.
            record_value = company_ticker.sustainability.T["totalEsg"]["Value"]

            p.produce(
                "esg",
                json.dumps(
                    {
                        "company": company,
                        "esg_score": record_value,
                        "time": str(dt.datetime.now()),
                    }
                ),
                callback=delivery_report,
            )
            p.flush()

        except Exception as e:
            # Send NaN-Value to ESG topic
            message = json.dumps(
                {
                    "company": company,
                    "esg_score": "NaN",
                    "time": str(dt.datetime.now()),
                }
            )
            p.produce("esg", message, callback=delivery_report)
            p.flush()

            # Send Error-Object to error topic (DLQ)
            error_message = json.dumps(
                {
                    "scraper": "esg",
                    "company": company,
                    "timestamp": str(dt.datetime.now()),
                    "error": repr(e),
                    "error_type": str(type(e)),
                }
            )
            p.produce("error", error_message, callback=delivery_report)
            p.flush()
            print(f"FAILED. For {company} the following error occured: {type(e)}")

        time.sleep(7)

    return "Done. Produced all ESGs to Kafka."

# We can extract every KPI for each NDXT company by changing the argument of the get_financial_KPI function.
# Instead of implementing five individual functions with the same structure, we only pass the KPI as an argument.
financial_KPIs = [
    #"Gross Profit",
    "EBIT",
    "Total Revenue",
    "Net Income",
    "Total Expenses",
]


# Convert dictionary keys from given data type to string
def convert_timestamps_keys_to_str(d: dict):
    return {str(k): v for k, v in d.items()}


def get_kpi_topic(kpi: str):
    # Because Confluent Kafka doesn't allow blank spaces within a topic name,
    # we replace the blank space in the kpi's name with an underscore.
    return kpi.lower().replace(" ", "_")


def get_financial_KPI(
    kpi: str, companies: list = companies, yearly_basis=True
):
    # Iterate over every company and extract gross profit development
    for company in companies:
        # Due to some performance issues
        time.sleep(12)
        try:
            company_ticker = yf.Ticker(f"{company}")
            # To create the value in a suitable way, we have to transpose the kpi data frame
            # in order to extract the kpi and its value over the years (since 2019).
            if yearly_basis:
                record_value = company_ticker.financials.T[kpi].to_dict()
            else:
                record_value = company_ticker.quarterly_financials.T[kpi].to_dict()

            # Because yfinance returns a DataFrame with timestamps as keys, we have to convert them into a string
            # since Timestamps won't work as json key
            record_value = convert_timestamps_keys_to_str(record_value)

            p.produce(
                get_kpi_topic(kpi),
                json.dumps(
                    {
                        "company": company,
                        f"{kpi}": record_value,
                        "time": str(dt.datetime.now()),
                    }
                ),
                callback=delivery_report,
            )
            p.flush()

        except Exception as e:
            # Send NaN-Value to kpi topic
            message = json.dumps(
                {
                    "company": company,
                    f"{kpi}": "NaN",
                    "time": str(dt.datetime.now()),
                }
            )
            p.produce(get_kpi_topic(kpi), message, callback=delivery_report)
            p.flush()

            # Send Error-Object to error topic (DLQ)
            error_message = json.dumps(
                {
                    "scraper": f"{kpi}",
                    "company": company,
                    "timestamp": str(dt.datetime.now()),
                    "error": repr(e),
                    "error_type": str(type(e)),
                }
            )
            p.produce("error", error_message, callback=delivery_report)
            p.flush()
            print(f"FAILED. For {company} the following error occured: {type(e)}")

    return f"Done. Produced {financial_KPIs} for all DAX40 companies to Kafka."

if __name__ == "__main__":

    # print("Now: All history Stock prices for all NDXT Companies ...")
    # get_history_stock_price()
    # time.sleep(5)

    print("Now: All finance KPIs for all NDXT Companies ...")
    for kpi in financial_KPIs:
        print(f"Now extracting {kpi}. Wait ...")
        print(f"Listing now all NDXT companies with all {kpi.upper()} values.\n")
        get_financial_KPI(kpi=kpi, yearly_basis=True)
        print("Done. Waiting for 120 seconds.")
        time.sleep(30)

    print("Now: ESG Score for all NDXT Companies ...")
    get_ESG_score()
    print("Done. Waiting for 5 seconds.")
    time.sleep(5)

