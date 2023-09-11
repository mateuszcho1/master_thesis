import requests
import json
import time

# Dane do logowania
login = 'login'
password = 'haslo!'

# Nazwa bazy danych
db_name = 'multi_1_mln_db'

# Adres serwera CouchDB
couchdb_url = 'http://127.0.0.1:5984/'

# Tworzymy sesję z autoryzacją
session = requests.Session()
session.auth = (login, password)

# Krok 1: Utwórz lub aktualizuj dokument design z funkcją mapującą
design_doc = {
    "_id": "_design/designDocName",
    "views": {
        "detailsFilter": {
            "map": """
            function(doc) {
                if (doc.orders) {
                    for (var i = 0; i < doc.orders.length; i++) {
                        var order = doc.orders[i];
                        if (order.details) {
                            for (var j = 0; j < order.details.length; j++) {
                                var detail = order.details[j];
                                if (detail.sub_total && detail.sub_total < 15) {
                                    emit(doc._id, doc);
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            """
        }
    }
}

# Spróbuj pobrać istniejący dokument design, jeśli istnieje
response = session.get(couchdb_url + db_name + "/_design/designDocName")
if response.status_code == 200:
    design_doc["_rev"] = response.json()["_rev"]

# Utwórz lub aktualizuj dokument design
session.put(couchdb_url + db_name + "/_design/designDocName", json=design_doc)

# Krok 2: Znajdź dokumenty z sub_total < 15 w ich zamówieniach używając widoku.
response = session.get(couchdb_url + db_name + "/_design/designDocName/_view/detailsFilter")

if response.status_code != 200:
    print(f"Problem z połączeniem z CouchDB: {response.status_code}")
    print(response.text)
    exit()

data = response.json()

try:
    documents_to_delete = [row['value'] for row in data['rows']]
except KeyError:
    print("Nie znaleziono klucza 'rows' w odpowiedzi z CouchDB:")
    print(data)
    exit()

# Krok 3: Zmierz czas.
start_time = time.time()

# Krok 4: Usuń te dokumenty.
for doc in documents_to_delete:
    delete_response = session.delete(couchdb_url + db_name + "/" + doc["_id"], headers={"If-Match": doc["_rev"]})
    if delete_response.status_code not in [200, 201, 202]:
        print(f"Problem z usuwaniem dokumentu o ID {doc['_id']}:")
        print(delete_response.json())

# Krok 5: Zakoncz pomiar czasu.
end_time = time.time()

# Krok 6: Przywróć usunięte dokumenty z kopii.
for doc in documents_to_delete:
    doc.pop("_rev", None)
    session.post(couchdb_url + db_name, json=doc)

# Podsumowanie
execution_time_ms = (end_time - start_time) * 1000  # Przeliczanie na milisekundy
print(f"Czas wykonania usuwania danych: {execution_time_ms:.2f} milisekund")
print(f"Liczba usuniętych dokumentów (a następnie przywróconych): {len(documents_to_delete)}")
