# Importowanie potrzebnych bibliotek
import mysql.connector
from mysql.connector import Error

# Utworzenie połączenia
try:
    cnx = mysql.connector.connect(user='root', password='haslo',
                                  host='localhost', database='Test_10mln_db')

    if cnx.is_connected():
        cursor = cnx.cursor()

        # Wykonanie zapytania
        cursor.execute("ALTER TABLE Client ADD PRIMARY KEY (id_client)")

        print("Klucz główny został dodany do kolumny id_client.")

except Error as e:
    print(f"Wystąpił błąd: {e}")

finally:
    if (cnx.is_connected()):
        cursor.close()
        cnx.close()
