import csv
import mysql.connector

# Stworzenie połączenia
cnx = mysql.connector.connect(user='root', password='haslo',
                              host='localhost')

cursor = cnx.cursor()

# Utworzenie bazy danych
cursor.execute("CREATE DATABASE IF NOT EXISTS Test_10tys_db")

# Wybranie bazy danych
cursor.execute("USE Test_10tys_db")

# Utworzenie tabeli
cursor.execute("""CREATE TABLE IF NOT EXISTS Client(
                  id_client INT,
                  name VARCHAR(255),
                  last_name VARCHAR(255),
                  nickname VARCHAR(255),
                  email VARCHAR(255),
                  country VARCHAR(255),
                  num_tel VARCHAR(255),
                  age INT)""")

# Wczytanie pliku CSV
with open('export_10tys.csv') as f:
    reader = csv.reader(f)
    next(reader)  # Pomija nagłówek
    for row in reader:
        # Wstawienie wiersza do tabeli
        cursor.execute(
            "INSERT INTO Client (id_client, name, last_name, nickname, email, country, num_tel, age) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            row
        )

# Zatwierdzenie transakcji
cnx.commit()

cursor.close()
cnx.close()