from sqlalchemy import create_engine, text
import time

# -------------------------------------------------------------------------------------------------------------------------
# Konfiguracja z MySQL

mysql_user = 'root'
mysql_password = 'haslo'
mysql_host = 'localhost'
mysql_database = 'multi_1_mln_db'

# Utworzenie silnika sqlalchemy dla MySQL
engine_mysql = create_engine(f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}")

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
with engine_mysql.begin() as transaction:
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
