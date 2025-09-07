import datetime
import logging
import os
import requests
from dotenv import load_dotenv
import pyodbc
import azure.functions as func
import pandas as pd
from sqlalchemy import create_engine

from sqlalchemy.exc import SQLAlchemyError
import sqlalchemy
from sqlalchemy import text


# ----------------------
# Configurations
# ----------------------
load_dotenv()
API_KEY = os.getenv("API_KEY")

def connect_to_database():
    """
    Creates a connection to the Azure SQL Database using SQLAlchemy.
    Uses environment variables for sensitive credentials.
    """
    server   = "eia-server.database.windows.net"
    database = "eiaDB"
    username = os.getenv("USERNAME_EIADB")
    password = os.getenv("PASSWORD_EIADB")  # Make sure it's set in your environment
    driver   = "ODBC Driver 17 for SQL Server"

    if not password:
        raise ValueError("Password not found in environment variable PASSWORD_EIADB")
    if not username : 
        raise ValueError("Username not found in environment variable USERNAME_EIADB")
    
    connection_string = (
        f"mssql+pyodbc://{username}:{password}@{server}/{database}"
        f"?driver={driver.replace(' ', '+')}"
    )

    try:
        engine = sqlalchemy.create_engine(connection_string)
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("""
                SELECT 1
            """))
        print("Connection to database successful")
        return engine
    except Exception as e:
        print("Failed to connect to database:", e)
        raise

# ----------------------
# Core functions
# ----------------------


def retrieve_and_clean_data_from_api(API_KEY: str, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Retrieve and clean data from the EIA API for a given date range.

    Parameters:
        API_KEY (str): EIA API key.
        start_date (str): Start date in 'YYYY-MM-DD' format.
        end_date (str): End date in 'YYYY-MM-DD' format.

    Returns:
        pd.DataFrame: Cleaned DataFrame containing the data, 
                      or empty DataFrame if request fails.
    """
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
        "api_key": API_KEY,
    }

    try:
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status()  # raises HTTPError for bad status codes

        data = response.json()

        # We check if we retrive the expected structure
        if "response" not in data or "data" not in data["response"]:
            raise ValueError("Unexpected API response structure")

        df = pd.DataFrame(data["response"]["data"])

        # We drop useless columns if they exist
        cols_to_drop = ["capacity-units", "outage-units", "percentOutage-units"]
        df.drop(columns=[c for c in cols_to_drop if c in df.columns], inplace=True, errors="ignore")

        # Convert types
        df["period"] = pd.to_datetime(df["period"], errors="coerce")
        df["capacity"] = pd.to_numeric(df["capacity"], errors="coerce")
        df["outage"] = pd.to_numeric(df["outage"], errors="coerce")
        df["percentOutage"] = pd.to_numeric(df["percentOutage"], errors="coerce")

        # Remove rows with missing critical values
        df.dropna(subset=["period", "percentOutage"], inplace=True)

        return df

    except requests.exceptions.RequestException as req_err:
        print(f"Request error: {req_err}")
    except ValueError as val_err:
        print(f"Data error: {val_err}")
    except Exception as e:
        print(f"Unexpected error: {e}")

    # If failure we return an empty DataFrame
    return pd.DataFrame()





def to_sql(df: pd.DataFrame, table_name: str, engine: sqlalchemy.engine.base.Engine) -> None:
    """
    Push a DataFrame to a SQL table with error handling.

    Parameters:
    df (pd.DataFrame): The DataFrame to push.
    table_name (str): The name of the target SQL table.
    engine (sqlalchemy.engine.base.Engine): SQLAlchemy engine connected to the database.

    Returns:
    None
    """
    try:
        if df.empty:
            print(f"Warning: DataFrame is empty. Nothing was inserted into '{table_name}'.")
            return

        df.to_sql(table_name, con=engine, if_exists="append", index=False)
        print(f"Successfully inserted {len(df)} rows into '{table_name}'.")

    except SQLAlchemyError as e:
        print(f"SQLAlchemy error while inserting into '{table_name}': {e}")

    except Exception as e:
        print(f"Unexpected error while inserting into '{table_name}': {e}")
        
# ----------------------
# Helper functions
# ----------------------


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
    
# -------------------
# Main program 
# -------------------

def main(mytimer: func.TimerRequest) -> None:
    logging.info("Function started")

    engine = connect_to_database()

    # Get last period in DB
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT TOP 1 period
            FROM Outages
            ORDER BY period DESC
        """))
        db_last_period = result.scalar()   


    if db_last_period is None:
        logging.warning("Database is empty, will insert all data")
        db_last_period = datetime.date(1900, 1, 1)  # very old default


    if isinstance(db_last_period, str):
        db_last_period = datetime.datetime.strptime(db_last_period, "%Y-%m-%d").date()

    # Compute the day after the last DB period
    start_date = db_last_period + datetime.timedelta(days=1)

    # Get today's date 
    today = datetime.date.today()

    # Retrieve data from API
    df_new = retrieve_and_clean_data_from_api(API_KEY, str(start_date), str(today))

    if df_new.empty:
        logging.info("No new data from API, nothing to insert")
        return

    # Check if last date in new data is already in DB
    api_last_period = pd.to_datetime(df_new["period"].max()).date()

    if api_last_period <= db_last_period:
        logging.info("API data already up to date with DB, nothing to insert")
        return

    # Otherwise insert new rows
    to_sql(df_new, "Outages", engine)
    logging.info("Data successfully inserted into Azure SQL")
