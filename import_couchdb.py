import requests
import json

# Dane do logowania
login = 'admin'
password = 'haslo'

# Nazwa bazy danych
db_name = 'multi_1_mln_db' 

# Adres serwera CouchDB
couchdb_url = 'http://127.0.0.1:5984/'

# Tworzymy sesję z autoryzacją
session = requests.Session()
session.auth = (login, password)

# Tworzenie bazy danych jeśli nie istnieje
r = session.put(couchdb_url + db_name)
if r.status_code != 201 and r.status_code != 202:
    print('Błąd podczas tworzenia bazy danych:', r.json())

# Odczytanie pliku JSON i importowanie danych
with open('multi1mln.json', 'r') as jsonfile:
    docs = json.load(jsonfile)

    # Dodajemy dokumenty do bazy danych partiami po 1000
    for i in range(0, len(docs), 1000):
        chunk = docs[i:i+1000]
        r = session.post(couchdb_url + db_name + '/_bulk_docs', json={'docs': chunk})
        if r.status_code != 201:
            print('Błąd podczas dodawania dokumentów:', r.json())
