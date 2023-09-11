from pymongo import MongoClient
import time

# Parametry połączenia
mongodb_uri = 'mongodb://localhost:27017/'
database_name = 'multi_1_mln_db'
collection_name = 'Client'

# Utworzenie połączenia
client = MongoClient(mongodb_uri)

# Wybór bazy danych
db = client[database_name]

# Wybór kolekcji
collection = db[collection_name]

# Krok 1: Znajdź dokumenty do aktualizacji.
clients_to_update = list(collection.find({"orders.total_price": {"$gt": 1500}}))

# Krok 2: Utwórz kopię tych dokumentów.
original_clients_data = [{**client} for client in clients_to_update]

# Krok 3: Zmierz czas.
start_time = time.time()

# Krok 4: Przeprowadź aktualizację.
for client in clients_to_update:
    new_nickname = client["nickname"] + "VIP"
    collection.update_one({"_id": client["_id"]}, {"$set": {"nickname": new_nickname}})

# Krok 5: Zakoncz pomiar czasu.
end_time = time.time()

# Krok 6: Przywróć dokumenty za pomocą kopii.
for original_client in original_clients_data:
    collection.replace_one({"_id": original_client["_id"]}, original_client)

# Podsumowanie
execution_time_ms = (end_time - start_time) * 1000  # Przeliczanie na milisekundy
print(f"Czas wykonania aktualizacji danych: {execution_time_ms:.2f} milisekund")
print(f"Liczba zmienionych dokumentów: {len(clients_to_update)}")
