# Importowanie potrzebnych bibliotek
import time
import mysql.connector
from mysql.connector import Error

# Utworzenie połączenia
try:
    cnx = mysql.connector.connect(user='root', password='haslo',
                                  host='localhost', database='Test_10tys_db')

    if cnx.is_connected():
        cursor = cnx.cursor()

        # Rozpocznij pomiar czasu
        start_time = time.time()

        # Wykonanie zapytania
        cursor.execute("UPDATE Client SET country = 'Poland' WHERE id_client = 5412")

        # # Skomplikowane zapytanie UPDATE
        # cursor.execute("UPDATE Client SET country = 'Germany' WHERE age >= 18 AND age <= 20")

        affected_rows = cursor.rowcount
        # Zakończenie pomiaru czasu
        end_time = time.time()

        # Obliczenie różnicy czasu w milisekundach
        execution_time = (end_time - start_time) * 1000

        # Pobranie liczby zmienionych rekordów
        print(f"Liczba zmienionych rekordów: {affected_rows}")

        print(f"Czas wykonania zapytania: {execution_time} ms")

        # Wycofanie transakcji
        cnx.rollback()

except Error as e:
    print(f"Wystąpił błąd: {e}")

finally:
    if (cnx.is_connected()):
        cursor.close()
        cnx.close()
