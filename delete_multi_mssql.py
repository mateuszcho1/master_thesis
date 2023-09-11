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

# Zapytanie do znalezienia szczegółów zamówienia, których wartość sub_total jest mniejsza niż 15
find_low_value_order_details_query = """
SELECT id_order_detail
FROM OrderDetail
WHERE sub_total < 15;
"""

# Zapytanie do usuwania rekordów
delete_order_details_query = """
DELETE FROM OrderDetail
WHERE id_order_detail = :id_order_detail;
"""

total_deleted_rows = 0
start_time = time.time()
with engine_mssql.begin() as transaction:
    try:
        # Pobranie listy szczegółów zamówienia do usunięcia
        order_details_to_delete = transaction.execute(text(find_low_value_order_details_query)).fetchall()
        
        for order_detail in order_details_to_delete:
            # Usuwanie rekordu
            result = transaction.execute(text(delete_order_details_query), {'id_order_detail': order_detail.id_order_detail})
            total_deleted_rows += result.rowcount

        end_time = time.time()  # zakończenie mierzenia czasu po usuwaniu
        transaction.rollback()  # Ręczny rollback, aby cofnąć zmiany
    except Exception as e:
        print(f"Błąd podczas usuwania danych: {e}")
        transaction.rollback()

execution_time_ms = (end_time - start_time) * 1000  # Przeliczanie na milisekundy
print(f"Czas wykonania usuwania danych: {execution_time_ms:.2f} milisekund")
print(f"Liczba usuniętych wierszy (zostały cofnięte): {total_deleted_rows}")
