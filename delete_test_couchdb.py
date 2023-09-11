import requests
import time
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

try:
    # Pobieranie dokumentów do usunięcia
    to_delete_docs_resp = session.post(couchdb_url + db_name + '/_find',
                                    json={
                                        "selector": {
                                            "age": {"$gt": 60},
                                            "country": "Poland"
                                        },
                                        "limit": 10000  # zwiększ limit dokumentów zwracanych na raz
                                    })

    to_delete_docs_resp.raise_for_status()
    to_delete_docs = to_delete_docs_resp.json()['docs']

    # Rozpocznij pomiar czasu
    start_time = time.time()

    # Wykonanie zapytania DELETE
    for doc in to_delete_docs:
        del_resp = session.delete(couchdb_url + db_name + '/' + doc['_id'] + '?rev=' + doc['_rev'])
        del_resp.raise_for_status()

    # Zakończenie pomiaru czasu
    end_time = time.time()

    # Obliczenie różnicy czasu w milisekundach
    execution_time = (end_time - start_time) * 1000

    print(f"Czas wykonania zapytania: {execution_time} ms")
    print(f"Liczba usuniętych dokumentów: {len(to_delete_docs)}")

    # Dodanie usuniętych dokumentów z powrotem do bazy danych
    for doc in to_delete_docs:
        del doc['_rev']  # usuń pole _rev, ponieważ teraz dodajemy jako nowy dokument
        add_resp = session.post(couchdb_url + db_name, json=doc)
        add_resp.raise_for_status()

except requests.HTTPError as e:
    print(f"Wystąpił błąd: {e}")
