import pandas as pd
from sqlalchemy import create_engine, text
import time

# -------------------------------------------------------------------------------------------------------------------------
# Konfiguracja z MySQL

mysql_user = 'root'
mysql_password = 'haslo'
mysql_host = 'localhost'
mysql_database = 'multi_1_mln_db'

engine_mysql = create_engine(f"mysql+mysqlconnector://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}")

# -------------------------------------------------------------------------------------------------------------------------
# Dodawanie nowego klienta, jego adresu oraz zamówienia w ramach transakcji

# Przygotowanie zapytań

insert_client_query = """
INSERT INTO Client(name, last_name, nickname, email, num_tel, age)
VALUES ('John', 'Doe', 'JohnD123', 'john.doe@example.com', '123-456-7890', 30);
"""

insert_address_query = """
INSERT INTO Address(id_client, street, city, state, zip_code)
VALUES (:client_id, '123 Main St', 'CityName', 'StateName', '12345');
"""

insert_order_query = """
INSERT INTO CustomerOrder(id_client, date_order, total_price)
VALUES (:client_id, NOW(), 300.50);
"""

insert_order_detail_query = """
INSERT INTO OrderDetail(id_order, id_product, quantity, sub_total)
VALUES (:order_id, 1, 2, 100.20), (:order_id, 2, 1, 100.15), (:order_id, 3, 1, 100.15);
"""

# Wykonanie zapytań w ramach transakcji i mierzenie czasu
start_time = time.time()

# Rozdziel zapytania
with engine_mysql.begin() as transaction:
    try:
        result = transaction.execute(text(insert_client_query))
        client_id = result.lastrowid

        transaction.execute(text(insert_address_query), {"client_id": client_id})
        
        order_result = transaction.execute(text(insert_order_query), {"client_id": client_id})
        order_id = order_result.lastrowid

        transaction.execute(text(insert_order_detail_query), {"order_id": order_id})

        end_time = time.time()  # zakończenie mierzenia czasu po dodaniu wszystkich danych

    except Exception as e:
        print(f"Błąd podczas dodawania danych: {e}")
        transaction.rollback()

execution_time_ms = (end_time - start_time) * 1000  # Przeliczanie na milisekundy
print(f"Czas wykonania dodawania danych: {execution_time_ms:.2f} milisekund")
