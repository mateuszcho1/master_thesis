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
        cursor.execute("START TRANSACTION")

        # Rozpocznij pomiar czasu
        start_time = time.time()

        # Wykonanie zapytania
        cursor.execute("INSERT INTO Client (id_client, name, last_name, nickname, email, country, num_tel, age) VALUES (100001, 'name', 'surname', 'namesurname', 'name.surname@gmail.com', 'Poland', '123-456-789', 26)")
        affected_rows = cursor.rowcount
        # Zakończenie pomiaru czasu
        end_time = time.time()

        # Obliczenie różnicy czasu w milisekundach
        execution_time = (end_time - start_time) * 1000

        # Pobranie liczby zmienionych rekordów
        print(f"Liczba dodanych rekordów: {affected_rows}")

        print(f"Czas wykonania zapytania: {execution_time} ms")

        # Wycofanie transakcji
        cnx.rollback()

except Error as e:
    print(f"Wystąpił błąd: {e}")

finally:
    if (cnx.is_connected()):
        cursor.close()
        cnx.close()
