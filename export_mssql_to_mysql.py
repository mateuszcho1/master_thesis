import pandas as pd
import urllib
from sqlalchemy import create_engine, inspect, text

# -------------------------------------------------------------------------------------------------------------------------
# Konfiguracja z MS SQL

# Parametry połączenia
server = 'DESKTOP-9F59L2S\SQLEXPRESS'
database = 'multi_1_mln_db'
driver = "{ODBC Driver 17 for SQL Server}"

# Utworzenie łańcucha połączenia
params = urllib.parse.quote_plus(f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes')

# Utworzenie silnika sqlalchemy dla MS SQL
engine_mssql = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

# -------------------------------------------------------------------------------------------------------------------------
# Konfiguracja z MySQL

mysql_user = 'root'
mysql_password = 'haslo'
mysql_host = 'localhost'
mysql_database = 'multi_1_mln_db'

engine_mysql = create_engine(f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}")

# -------------------------------------------------------------------------------------------------------------------------
# Przenoszenie danych

tables = ['Client', 'Address', 'Product', 'CustomerOrder', 'OrderDetail']
chunk_size = 10000  # rozmiar bloku

for table in tables:
    df = pd.read_sql(f"SELECT * FROM [dbo].[{table}]", engine_mssql)
    number_of_chunks = len(df) // chunk_size + 1

    connection = engine_mysql.connect()

    for i in range(number_of_chunks):
        start_index = i * chunk_size
        end_index = (i + 1) * chunk_size
        sub_df = df.iloc[start_index:end_index]

        trans = connection.begin()
        try:
            sub_df.to_sql(table, connection, if_exists='append', index=False)
            trans.commit()
            print(f"Chunk {i + 1}/{number_of_chunks} of table {table} successfully inserted!")
        except Exception as e:
            print(f"Error with chunk {i + 1}/{number_of_chunks} of table {table}: {str(e)}")
            trans.rollback()
        finally:
            if i == number_of_chunks - 1:
                connection.close()
