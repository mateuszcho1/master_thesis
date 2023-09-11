import pandas as pd
import urllib
from sqlalchemy import create_engine, text
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
# Dodawanie nowego klienta, jego adresu oraz zamówienia w ramach transakcji

# Przygotowanie zapytań
insert_client_query = """
INSERT INTO Client(name, last_name, nickname, email, num_tel, age)
VALUES ('John', 'Doe', 'JohnD123', 'john.doe@example.com', '123-456-7890', 30);
"""

insert_address_query = """
INSERT INTO Address(id_client, street, city, state, zip_code)
VALUES (SCOPE_IDENTITY(), '123 Main St', 'CityName', 'StateName', '12345');
"""

insert_order_query = """
INSERT INTO CustomerOrder(id_client, date_order, total_price)
VALUES (SCOPE_IDENTITY(), GETDATE(), 300.50);
"""

insert_order_detail_query = """
DECLARE @order_id int = SCOPE_IDENTITY();

INSERT INTO OrderDetail(id_order, id_product, quantity, sub_total)
VALUES (@order_id, 1, 2, 100.20), (@order_id, 2, 1, 100.15), (@order_id, 3, 1, 100.15);
"""

# Wykonanie zapytań w ramach transakcji i mierzenie czasu
start_time = time.time()

with engine_mssql.begin() as transaction:
    try:
        transaction.execute(text(insert_client_query))
        transaction.execute(text(insert_address_query))
        transaction.execute(text(insert_order_query))
        transaction.execute(text(insert_order_detail_query))

        end_time = time.time()  # zakończenie mierzenia czasu po dodaniu wszystkich danych
    except Exception as e:
        print(f"Błąd podczas dodawania danych: {e}")
        transaction.rollback()

execution_time_ms = (end_time - start_time) * 1000  # Przeliczanie na milisekundy
print(f"Czas wykonania dodawania danych: {execution_time_ms:.2f} milisekund")
