import requests
import json
import time

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

# Definicja widoku
view_definition = {
    "_id": "_design/clients_with_subtotal_range",
    "views": {
        "by_subtotal_range": {
            "map": """function(doc) {
                doc.addresses.forEach(function(address) {
                    doc.orders.forEach(function(order) {
                        order.details.forEach(function(detail) {
                            if (detail.sub_total >= 100 && detail.sub_total <= 200) {
                                emit([doc.name, doc._id], {
                                    "id_client": doc._id,
                                    "client_name": doc.name,
                                    "client_last_name": doc.last_name,
                                    "street": address.street,
                                    "city": address.city,
                                    "state": address.state,
                                    "date_order": order.date_order,
                                    "product_name": detail.product_name,
                                    "quantity": detail.quantity,
                                    "sub_total": detail.sub_total
                                });
                            }
                        });
                    });
                });
            }"""
        }
    }
}

# Rozpoczęcie pomiaru czasu
start_time = time.time()

# Spróbuj dodać widok
response = session.put(couchdb_url + db_name + "/_design/clients_with_subtotal_range", json=view_definition)
if response.status_code not in [201, 202]:
    print('Błąd podczas tworzenia widoku:', response.json())

# Pobierz dane z widoku
response = session.get(couchdb_url + db_name + "/_design/clients_with_subtotal_range/_view/by_subtotal_range?limit=268435455")
if response.status_code == 200:
    documents = response.json().get("rows", [])
    print(f"Liczba zwróconych dokumentów: {len(documents)}")
else:
    print("Błąd podczas pobierania danych:", response.json())

# Zakończenie pomiaru czasu
end_time = time.time()

execution_time_ms = (end_time - start_time) * 1000  # Przeliczanie na milisekundy
print(f"Czas wykonania zapytania: {execution_time_ms:.2f} milisekund")

# Usunięcie widoku
rev_response = session.get(couchdb_url + db_name + "/_design/clients_with_subtotal_range")
if rev_response.status_code == 200:
    current_rev = rev_response.json().get("_rev", None)
else:
    print("Błąd podczas pobierania _rev dla designu:", rev_response.json())
    current_rev = None

if current_rev:
    delete_response = session.delete(couchdb_url + db_name + "/_design/clients_with_subtotal_range?rev=" + current_rev)
    if delete_response.status_code not in [200, 202]:
        print('Błąd podczas usuwania widoku:', delete_response.json())
