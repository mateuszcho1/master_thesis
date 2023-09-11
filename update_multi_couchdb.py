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

# Krok 1: Znajdź dokumenty do aktualizacji.
response = session.get(couchdb_url + db_name + "/_all_docs?include_docs=true")

data = response.json()
clients_to_update = [row['doc'] for row in data['rows'] if any(order['total_price'] > 1500 for order in row['doc'].get('orders', []))]

# Krok 2: Utwórz kopię tych dokumentów.
original_clients_data = [{**client} for client in clients_to_update]

# Krok 3: Zmierz czas.
start_time = time.time()

# Krok 4: Przeprowadź aktualizację.
for client in clients_to_update:
    new_nickname = client["nickname"] + "VIP"
    client["nickname"] = new_nickname
    update_response = session.put(couchdb_url + db_name + "/" + client["_id"], json=client)
    if update_response.status_code != 201:
        print(f"Problem z aktualizacją dokumentu o ID {client['_id']}:")
        print(update_response.json())
    continue

# Krok 5: Zakoncz pomiar czasu.
end_time = time.time()

# Krok 6: Przywróć dokumenty za pomocą kopii.
for original_client in original_clients_data:
    session.put(couchdb_url + original_client["_id"], json=original_client)

# Podsumowanie
execution_time_ms = (end_time - start_time) * 1000  # Przeliczanie na milisekundy
print(f"Czas wykonania aktualizacji danych: {execution_time_ms:.2f} milisekund")
print(f"Liczba zmienionych dokumentów: {len(clients_to_update)}")
