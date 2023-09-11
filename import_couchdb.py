import csv
import requests
import json

# Dane do logowania
login = 'admin'
password = 'haslo'

# Nazwa bazy danych
db_name = 'test1mlndb' 

# Adres serwera CouchDB
couchdb_url = 'http://127.0.0.1:5984/'

# Tworzymy sesję z autoryzacją
session = requests.Session()
session.auth = (login, password)

# Tworzenie bazy danych jeśli nie istnieje
r = session.put(couchdb_url + db_name)
if r.status_code != 201 and r.status_code != 202:
    print('Błąd podczas tworzenia bazy danych:', r.json())

# Odczytanie pliku CSV i importowanie danych
docs = []  # Lista dokumentów do dodania
with open('export_1mln.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        row['_id'] = row['id_client']
        docs.append(row)

        # Dodajemy dokumenty do bazy danych partiami po 1000
        if len(docs) >= 1000:
            r = session.post(couchdb_url + db_name + '/_bulk_docs', json={'docs': docs})
            if r.status_code != 201:
                print('Błąd podczas dodawania dokumentów:', r.json())
            docs = []

    # Dodajemy pozostałe dokumenty, jeśli jakieś zostały
    if docs:
        r = session.post(couchdb_url + db_name + '/_bulk_docs', json={'docs': docs})
        if r.status_code != 201:
            print('Błąd podczas dodawania dokumentów:', r.json())
