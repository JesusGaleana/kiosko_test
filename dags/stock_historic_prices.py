#AIRFLOW libraries
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

#Business Logic libraries
from datetime import datetime, timedelta, date
from utils.postgres_db import PostgreSQLConnection
import pandas as pd
import os

def fill_dim_dates_func():
    # Fecha de inicio y fin
    start_date = date(2013, 2, 8)
    end_date = date(2024, 12, 31)

    # Consultas de inserción
    insert_queries = []

    # Generar consultas de inserción para cada fecha
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append([current_date.strftime("%Y-%m-%d"), str(current_date.year), str(current_date.month), str(current_date.day)])
        current_date += timedelta(days=1)
    
    df_dates_source = pd.DataFrame(date_list, columns=['date', 'year', 'month', 'day'])

    # Get connection from Database
    connection = PostgreSQLConnection(
        host = os.getenv("HOST_DB"),
        port= os.getenv("PORT_DB"),
        database = os.getenv("DB_NAME"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD")
    )
    connection.connect()

    # Get date data
    consulta = "SELECT * FROM mi_kiosko.dim_date;"
    datos = connection.execute_query(consulta)
    # Convertir los datos en un DataFrame de Pandas
    if datos:
        # Obtener los nombres de las columnas
        nombres_columnas = [desc[0] for desc in connection.cursor.description]
        # Crear el DataFrame
        df_dates = pd.DataFrame(datos, columns=nombres_columnas)
        # Imprimir el DataFrame
        df_dates = df_dates[["date", "sk_date"]]
        df_dates_source['date'] = pd.to_datetime(df_dates_source['date'])
        df_dates['date'] = pd.to_datetime(df_dates['date'])
        df_result = df_dates_source[~df_dates_source['date'].isin(df_dates['date'])]
        final_list = df_result.values.tolist()
    else: 
        final_list = date_list

    # Create a list of rows to be inserted and declarate the columns where the data to be inserted
    if final_list:
        columns = ["date", "year", "month", "day"]
        connection.insert_query(table="mi_kiosko.dim_date", columns=columns, values=final_list)

    connection.disconnect()


def fill_stock_historic_func():
    # Get connection from Database
    connection = PostgreSQLConnection(
        host = os.getenv("HOST_DB"),
        port= os.getenv("PORT_DB"),
        database = os.getenv("DB_NAME"),
        user = os.getenv("DB_USER"),
        password = os.getenv("DB_PASSWORD")
    )
    connection.connect()
    
    # Read historical prices
    df_historical = pd.read_csv("/opt/data_files/all_stocks_5yr.csv")

    # Get date data to cross with historical dates
    query = "SELECT * FROM mi_kiosko.dim_date;"
    date_data = connection.execute_query(query)
    if date_data:
        # Get column names
        column_names = [desc[0] for desc in connection.cursor.description]
        # Generate dataframe with date data
        df_dates = pd.DataFrame(date_data, columns=column_names)
        # Select columns to join data
        df_dates = df_dates[["date", "sk_date"]]
    

    # Convert date types to homologate for join
    df_historical['date'] = pd.to_datetime(df_historical['date'])
    df_dates['date'] = pd.to_datetime(df_dates['date'])

    # Join date data for get foreign keys
    df_result = pd.merge(df_historical, df_dates, on='date', how='left')

    # Get stock symbol and drop duplicates for load into dimension table
    stock_column = df_result['Name'].drop_duplicates()
    # Add null elements for other columns in table, TODO add stocks information in the future
    stock_list = [[stock, None, None, None, None] for stock in stock_column]

    # Get stock data from dimension to validate if exist the stock in dimension table
    query = "SELECT * FROM mi_kiosko.dim_stock;"
    stock_data = connection.execute_query(query)
    if stock_data:
        # Get column names
        column_names = [desc[0] for desc in connection.cursor.description]
        # Generate dataframe with stock data
        df_stocks = pd.DataFrame(stock_data, columns=column_names)
        df_stocks_source = pd.DataFrame(stock_list, columns=["symbol", "company_name", "sector", "country_iso", "currency"])
        # Select columns to join data with source dataframe
        df_stocks = df_stocks[["symbol", "sk_stock"]]
        df_stocks_load = df_stocks_source[~df_stocks_source['symbol'].isin(df_stocks['symbol'])]
        final_list = df_stocks_load.values.tolist()
    else: 
        final_list = stock_list

    # If exist stocks to load, insert in dimension table for stocks
    if final_list:
        columns = ["symbol", "company_name", "sector", "country_iso", "currency"]
        connection.insert_query(table="mi_kiosko.dim_stock", columns=columns, values=final_list)

    
    # Get stock data for join with historical prices
    query = "SELECT * FROM mi_kiosko.dim_stock;"
    stock_data = connection.execute_query(query)
    # Convertir los datos en un DataFrame de Pandas
    if stock_data:
        # Get column names
        column_names = [desc[0] for desc in connection.cursor.description]
        # Generate dataframe with stock data
        df_stocks = pd.DataFrame(stock_data, columns=column_names)
        # Select columns to join data with source dataframe
        df_stocks = df_stocks[["symbol", "sk_stock"]]
    
    df_result = pd.merge(df_result, df_stocks, left_on='Name', right_on='symbol', how='left')
    df_result = df_result[["sk_stock", "sk_date", "open", "high", "low", "close", "volume"]]

    list_result = df_result.values.tolist()

    if list_result:
        columns = ["fk_stock", "fk_date", "open_price", "high_price", "low_price", "close_price", "volume"]
        connection.insert_query(table="mi_kiosko.fact_price_history", columns=columns, values=list_result)

    connection.disconnect()


########
########
#AIRFLOW DAG Creator
########
########

# Define los argumentos predeterminados del DAG.
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 5, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define el DAG.
dag = DAG(
    'stock_historic_prices',
    default_args=default_args,
    description='Load data into stock prices data mart',
    schedule_interval='@daily',
)

# Define el operador para ejecutar el script de Python.
fill_dim_dates_task = PythonOperator(
    task_id='fill-dim_dates',
    python_callable=fill_dim_dates_func,  # my_script es el nombre de tu script Python y main es la función que deseas llamar
    dag=dag,
)
fill_stock_history_task = PythonOperator(
    task_id='fill-stock_history',
    python_callable=fill_stock_historic_func,  # my_script es el nombre de tu script Python y main es la función que deseas llamar
    dag=dag,
)

fill_dim_dates_task >> fill_stock_history_task