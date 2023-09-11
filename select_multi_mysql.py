import pandas as pd
from sqlalchemy import create_engine
import time

# -------------------------------------------------------------------------------------------------------------------------
# Konfiguracja z MySQL

mysql_user = 'root'
mysql_password = 'haslo'
mysql_host = 'localhost'
mysql_database = 'multi_1_mln_db'

engine_mysql = create_engine(f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}")

# -------------------------------------------------------------------------------------------------------------------------
# Przygotowanie zapytania SQL
query = """
SELECT SQL_NO_CACHE
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
df = pd.read_sql(query, engine_mysql)
end_time = time.time()

execution_time_ms = (end_time - start_time) * 1000  # Przeliczanie na milisekundy
print(f"Czas wykonania zapytania: {execution_time_ms:.2f} milisekund")
print(f"Liczba zwróconych wierszy: {len(df)}")
