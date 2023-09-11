import time
import pandas as pd
import urllib
from sqlalchemy import create_engine
from sqlalchemy.sql import text

# Parametry połączenia
server = 'DESKTOP-9F59L2S\SQLEXPRESS'
database = 'Test_10tys_db'
driver = "{ODBC Driver 17 for SQL Server}"

# Utworzenie łańcucha połączenia
params = urllib.parse.quote_plus(f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes')

# Utworzenie silnika sqlalchemy
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

# Utworzenie połączenia
conn = engine.connect()

# Rozpoczęcie transakcji
trans = conn.begin()

try:
    # Rozpocznij pomiar czasu
    start_time = time.time()

    # # Proste zapytanie UPDATE
    # stmt = text("UPDATE Client SET country = 'Poland' WHERE id_client = 115412")
    # result = conn.execute(stmt)

    # Zlozone zapytanie UPDATE
    stmt = text("UPDATE Client SET country = 'Germany' WHERE age >= 18 AND age <= 20")
    result = conn.execute(stmt)


    # Zakończenie pomiaru czasu
    end_time = time.time()

    # Obliczenie różnicy czasu w milisekundach
    execution_time = (end_time - start_time) * 1000
    print(f"Czas wykonania zapytania: {execution_time} ms")
    print(f"Liczba zmienionych rekordów: {result.rowcount}")

    # Wycofanie transakcji
    trans.rollback()

except Exception as e:
    print(f"Wystąpił błąd: {e}")
    # Wycofanie transakcji w przypadku błędu
    trans.rollback()

finally:
    # Zamknięcie połączenia
    conn.close()
