# Importowanie potrzebnych bibliotek
import time
import mysql.connector
from mysql.connector import Error

# Utworzenie połączenia
try:
    cnx = mysql.connector.connect(user='root', password='haslo',
                                  host='localhost', database='Test_100tys_db')

    if cnx.is_connected():
        cursor = cnx.cursor()

        # Rozpoczęcie transakcji
        cnx.start_transaction()

        # Rozpocznij pomiar czasu
        start_time = time.time()

# ===================================================================

        # # Wykonanie zapytania PROSTY SELECT
        # cursor.execute("SELECT * FROM Client WHERE country = 'Finland'")

        # Wykonanie zapytania ZLOZONE ZAPYTANIE
        cursor.execute("SELECT * FROM Client WHERE country = 'Estonia' AND age > 35 ORDER BY age ASC")

# ===================================================================

        # Pobranie wszystkich wyników
        rows = cursor.fetchall()

        # Zakończenie pomiaru czasu
        end_time = time.time()

        # Obliczenie różnicy czasu w milisekundach
        execution_time = (end_time - start_time) * 1000
        print(f"Czas wykonania zapytania: {execution_time} ms")
        print(f"Liczba wyników: {len(rows)}")

        # Wycofanie transakcji
        cnx.rollback()

except Error as e:
    print(f"Wystąpił błąd: {e}")

finally:
    if (cnx.is_connected()):
        cursor.close()
        cnx.close()
