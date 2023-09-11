# Biblioteki
import time
import pandas as pd
import urllib
from sqlalchemy import create_engine
from sqlalchemy.sql import text

# -------------------------------------------------------------------------------------------------------------------------
# Konfguracja z MS SQL

# Parametry połączenia
server = 'DESKTOP-9F59L2S\SQLEXPRESS'
database = 'Test_10mln_db'
driver = "{ODBC Driver 17 for SQL Server}"

# Utworzenie łańcucha połączenia
params = urllib.parse.quote_plus(f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes')

# Utworzenie silnika sqlalchemy
engine = create_engine(f'mssql+pyodbc:///?odbc_connect={params}')

# Utworzenie połączenia
conn = engine.connect()

# Rozpoczęcie transakcji
trans = conn.begin()

# --------------------------------------------------------------------------

try:
    # Rozpocznij pomiar czasu
    start_time = time.time()

# ===================================================================================================

    # # Wykonanie zapytania PROSTEGO SELECTA
    # stmt = text("SELECT * FROM Client WHERE country = 'Finland'")

    # Wykonanie zapytania ZLOZONEGO SELECTA
    stmt = text("SELECT * FROM Client WHERE country = 'Estonia' AND age > 35 ORDER BY age ASC")

# ===================================================================================================


    result = conn.execute(stmt)

    # Pobranie wszystkich wyników
    rows = result.fetchall()

    # Zakończenie pomiaru czasu
    end_time = time.time()

    # Obliczenie różnicy czasu w milisekundach
    execution_time = (end_time - start_time) * 1000
    print(f"Czas wykonania zapytania: {execution_time} ms")
    print(f"Liczba wyników: {len(rows)}")

    # Wycofanie transakcji
    trans.rollback()

except Exception as e:
    print(f"Wystąpił błąd: {e}")
    # Wycofanie transakcji w przypadku błędu
    trans.rollback()

finally:
    # Zamknięcie połączenia
    conn.close()
