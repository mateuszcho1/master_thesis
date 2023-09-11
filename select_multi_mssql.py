import pandas as pd
import urllib
from sqlalchemy import create_engine, inspect, text
import time

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
# Przygotowanie zapytania
query = """
SELECT 
    c.id_client,
    c.name AS client_name,
    c.last_name AS client_last_name,
    a.street,
    a.city,
    a.state,
    o.date_order,
    p.product_name,
    od.quantity,
    od.sub_total
FROM 
    Client AS c
JOIN 
    Address AS a ON c.id_client = a.id_client
JOIN 
    CustomerOrder AS o ON c.id_client = o.id_client
JOIN 
    OrderDetail AS od ON o.id_order = od.id_order
JOIN 
    Product AS p ON od.id_product = p.id_product
WHERE 
    od.sub_total BETWEEN 100 AND 200;
"""

# Pobieranie danych i mierzenie czasu wykonania zapytania
start_time = time.time()
df = pd.read_sql(query, engine_mssql)
end_time = time.time()

execution_time_ms = (end_time - start_time) * 1000  # Przeliczanie na milisekundy
print(f"Czas wykonania zapytania: {execution_time_ms:.2f} milisekund")
print(f"Liczba zwróconych wierszy: {len(df)}")
