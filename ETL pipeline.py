# Airflow DAG for fetching stock data and loading it into Snowflake
from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
import yfinance as yf
import pandas as pd
import snowflake.connector
from airflow.models import Variable

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    description='Fetch stock data and load it into Snowflake',
    schedule_interval="@daily",
    catchup=False
)
def etl_pipeline():
    @task
    def fetch_stock_data(symbols):
        """Fetch stock data from Yahoo Finance"""
        data_frames = []
        for symbol in symbols:
            stock = yf.Ticker(symbol)
            df = stock.history(period="180d")
            df['Symbol'] = symbol
            df = df[['Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']]
            df.reset_index(inplace=True)
            data_frames.append(df)
        return pd.concat(data_frames)

    @task
    def load_to_snowflake(stock_data_df):
        """Load data into Snowflake"""
        conn = snowflake.connector.connect(
            user=Variable.get("username"),
            password=Variable.get("password"),
            account=Variable.get("account"),
            warehouse="compute_wh",
            database="dev"
        )
        cursor = conn.cursor()
        target_table = "dev.raw.stock_data"
        try:
            cursor.execute("BEGIN;")
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {target_table} (
                    symbol VARCHAR,
                    date DATE,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    volume BIGINT,
                    PRIMARY KEY (symbol, date)
                );
            """)
            cursor.execute(f"DELETE FROM {target_table}")
            for _, row in stock_data_df.iterrows():
                sql = f"""
                    INSERT INTO {target_table} (symbol, date, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    row['Symbol'],
                    row['Date'].strftime('%Y-%m-%d'),
                    float(row['Open']),
                    float(row['High']),
                    float(row['Low']),
                    float(row['Close']),
                    int(row['Volume'])
                ))
            cursor.execute("COMMIT;")
        except Exception as e:
            cursor.execute("ROLLBACK;")
            raise e
        finally:
            cursor.close()
            conn.close()

    symbols = ["NVDA", "AAPL"]
    stock_data = fetch_stock_data(symbols)
    load_to_snowflake(stock_data)

etl_dag = etl_pipeline()
# Airflow DAG for fetching stock data and loading it into Snowflake
from airflow import DAG
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago
from datetime import timedelta
import yfinance as yf
import pandas as pd
import snowflake.connector
from airflow.models import Variable

# Default arguments for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    default_args=default_args,
    description='Fetch stock data and load it into Snowflake',
    schedule_interval="@daily",
    catchup=False
)
def etl_pipeline():
    @task
    def fetch_stock_data(symbols):
        """Fetch stock data from Yahoo Finance"""
        data_frames = []
        for symbol in symbols:
            stock = yf.Ticker(symbol)
            df = stock.history(period="180d")
            df['Symbol'] = symbol
            df = df[['Symbol', 'Open', 'High', 'Low', 'Close', 'Volume']]
            df.reset_index(inplace=True)
            data_frames.append(df)
        return pd.concat(data_frames)

    @task
    def load_to_snowflake(stock_data_df):
        """Load data into Snowflake"""
        conn = snowflake.connector.connect(
            user=Variable.get("username"),
            password=Variable.get("password"),
            account=Variable.get("account"),
            warehouse="compute_wh",
            database="dev"
        )
        cursor = conn.cursor()
        target_table = "dev.raw.stock_data"
        try:
            cursor.execute("BEGIN;")
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {target_table} (
                    symbol VARCHAR,
                    date DATE,
                    open FLOAT,
                    high FLOAT,
                    low FLOAT,
                    close FLOAT,
                    volume BIGINT,
                    PRIMARY KEY (symbol, date)
                );
            """)
            cursor.execute(f"DELETE FROM {target_table}")
            for _, row in stock_data_df.iterrows():
                sql = f"""
                    INSERT INTO {target_table} (symbol, date, open, high, low, close, volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                cursor.execute(sql, (
                    row['Symbol'],
                    row['Date'].strftime('%Y-%m-%d'),
                    float(row['Open']),
                    float(row['High']),
                    float(row['Low']),
                    float(row['Close']),
                    int(row['Volume'])
                ))
            cursor.execute("COMMIT;")
        except Exception as e:
            cursor.execute("ROLLBACK;")
            raise e
        finally:
            cursor.close()
            conn.close()

    symbols = ["NVDA", "AAPL"]
    stock_data = fetch_stock_data(symbols)
    load_to_snowflake(stock_data)

etl_dag = etl_pipeline()
