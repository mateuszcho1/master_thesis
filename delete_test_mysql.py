import time
import mysql.connector
from mysql.connector import Error

# Utworzenie połączenia
try:
    cnx = mysql.connector.connect(user='root', password='haslo',
                                  host='localhost', database='Test_10mln_db')

    if cnx.is_connected():
        cursor = cnx.cursor()

        # Rozpoczęcie transakcji
        cnx.start_transaction()

        # Rozpocznij pomiar czasu
        start_time = time.time()

        # Skomplikowane zapytanie DELETE
        cursor.execute("DELETE FROM Client WHERE age > 60 AND country = 'Poland'")

        # Zakończenie pomiaru czasu
        end_time = time.time()

        # Obliczenie różnicy czasu w milisekundach
        execution_time = (end_time - start_time) * 1000
        print(f"Czas wykonania zapytania: {execution_time} ms")
        print(f"Liczba usuniętych rekordów: {cursor.rowcount}")

        # Wycofanie transakcji
        cnx.rollback()

except Error as e:
    print(f"Wystąpił błąd: {e}")

finally:
    if (cnx.is_connected()):
        cursor.close()
        cnx.close()
