import pandas as pd
import urllib
from sqlalchemy import create_engine, text
import time

# Konfiguracja z MS SQL

# Parametry połączenia
server = 'DESKTOP-9F59L2S\SQLEXPRESS'
database = 'multi_1_mln_db'
driver = "{ODBC Driver 17 for SQL Server}"

# Utworzenie łańcucha połączenia
params = urllib.parse.quote_plus(f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes')

# Utworzenie silnika sqlalchemy dla MS SQL
engine_mssql = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

# Zapytanie do znalezienia klientów, których wartość zamówienia przekracza 1500
find_high_value_clients_query = """
SELECT c.id_client, c.nickname
FROM Client c
INNER JOIN CustomerOrder o ON c.id_client = o.id_client
WHERE o.total_price > 1500;
"""

# Przygotowanie transakcji do aktualizacji
update_query_template = """
UPDATE Client
SET nickname = :new_nickname
WHERE id_client = :id_client;
"""

# ...

total_updated_rows = 0
start_time = time.time()
with engine_mssql.begin() as transaction:
    try:
        # Pobranie listy klientów do aktualizacji
        clients_to_update = transaction.execute(text(find_high_value_clients_query)).fetchall()
        
        for client in clients_to_update:
            # Aktualizacja nickname dla każdego klienta
            new_nickname = client.nickname + "VIP"
            result = transaction.execute(text(update_query_template), {'id_client': client.id_client, 'new_nickname': new_nickname})
            total_updated_rows += result.rowcount

        end_time = time.time()  # zakończenie mierzenia czasu po aktualizacji
        transaction.rollback()  # Dodanie ręcznego rollback na końcu bloku try
    except Exception as e:
        end_time = time.time()  # zapewnia zakończenie mierzenia czasu w przypadku błędu
        print(f"Błąd podczas aktualizacji danych: {e}")
        transaction.rollback()


execution_time_ms = (end_time - start_time) * 1000  # Przeliczanie na milisekundy
print(f"Czas wykonania aktualizacji danych: {execution_time_ms:.2f} milisekund")
print(f"Liczba zmienionych wierszy: {total_updated_rows}")

