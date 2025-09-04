import datetime
import logging
import os
import requests
import pyodbc
import azure.functions as func
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy
from sqlalchemy import text

server   = "eia-server.database.windows.net"  
database = "eiaDB"               # 
username = "mathis"              
password = os.getenv("PASSWORD_EIADB")                    
driver   = "ODBC Driver 17 for SQL Server"

connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={driver.replace(' ', '+')}"
engine = sqlalchemy.create_engine(connection_string)



API_KEY = os.getenv("API_KEY")

def retrieve_and_clean_data_from_api(API_KEY: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Retrieve and clean data from the EIA API for a given date range.

    Parameters:
    API_KEY (str): EIA API key.
    start_date (str): The start date in 'YYYY-MM-DD' format.
    end_date (str): The end date in 'YYYY-MM-DD' format.


    Returns:
    pd.DataFrame: Cleaned DataFrame containing the data.
    """

    # Url and params to do the request to the API
    url = "https://api.eia.gov/v2/nuclear-outages/generator-nuclear-outages/data/"

    params = {
        "frequency": "daily",
        "data[0]": "capacity",
        "data[1]": "outage",
        "data[2]": "percentOutage",
        "start": start_date,
        "end": end_date,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "api_key": API_KEY
    }

    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        
        # We retrieve the data 
        data_list = data['response']['data']

        # Convert to pandas DataFrame
        df = pd.DataFrame(data_list)

        # We delete the columns that are useless 
        df.drop(columns=["capacity-units","outage-units","percentOutage-units"], inplace=True)

        # We convert the columns to the right type
        df["period"] = pd.to_datetime(df["period"])
        df["capacity"] = pd.to_numeric(df["capacity"])
        df["outage"] = pd.to_numeric(df["outage"])
        df["percentOutage"] = pd.to_numeric(df["percentOutage"])

        return df
    
    else:
        print(f"Error: {response.status_code}")
        print(response.text)
        return pd.DataFrame()
    


def to_sql(df: pd.DataFrame, table_name: str, engine: sqlalchemy.engine.base.Engine) -> None:
    """
    Push a DataFrame to a SQL table.

    Parameters:
    df (pd.DataFrame): The DataFrame to push.
    table_name (str): The name of the target SQL table.
    engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine connected to the database.

    Returns:
    None
    """
    df.to_sql(table_name, con=engine, if_exists="append", index=False)


def is_requests_over_5000(start_date: str, end_date: str) -> bool:
    """
    Check if the number of requests to the API exceeds 5000 for a given date range.

    Parameters:
    start_date (str): The start date in 'YYYY-MM-DD' format.
    end_date (str): The end date in 'YYYY-MM-DD' format.

    Remark : 
    There are currently 94 generators in the API, so a day corresponds to 94 lines to reach 5000 requests, we need about 53 days.

    Returns:
    bool: True if requests exceed 5000, False otherwise.
    """
    start = datetime.datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.datetime.strptime(end_date, "%Y-%m-%d")
    delta = end - start
    return delta.days * 94 > 5000
    


def main(mytimer: func.TimerRequest) -> None:
    pass
    logging.info("Function started")

    QUERY = """
    SELECT TOP 1 period
    FROM Outages
    ORDER BY period DESC
    """

    # Here we select the last day we have data in the database to not have duplicates
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT TOP 1 period
            FROM Outages
            ORDER BY period DESC
        """))
        str_last_period = str(result.scalar())   # fetch the first column of the first row

    
    # We add one day to the last period to not have duplicates
    last_period_date = datetime.datetime.strptime(str_last_period, "%Y-%m-%d")
    last_period_date += datetime.timedelta(days=1)
    str_last_period = str(last_period_date.date())

    # We get today's date 
    today_str = str(datetime.date.today())

    df_last_period_to_today = retrieve_and_clean_data_from_api(API_KEY, str_last_period, today_str)

    # Check for duplicates 


    to_sql(df_last_period_to_today, "Outages", engine)

    logging.info("Data successfully inserted into Azure SQL")
