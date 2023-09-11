from pymongo import MongoClient
import time

# Parametry połączenia
mongodb_uri = 'mongodb://localhost:27017/'
database_name = 'multi_100_tys_db'
collection_name = 'Client'

# Utworzenie połączenia
client = MongoClient(mongodb_uri)

# Wybór bazy danych
db = client[database_name]

# Wybór kolekcji
collection = db[collection_name]

# Krok 1: Znajdź i zapisz kopię dokumentów, które mają sub_total < 15 w ich zamówieniach.
documents_to_delete = list(collection.find({
    "orders.details": {
        "$elemMatch": {
            "sub_total": {
                "$lt": 15
            }
        }
    }
}))

# Krok 2: Zmierz czas.
start_time = time.time()

# Krok 3: Usuń te dokumenty.
result = collection.delete_many({
    "orders.details": {
        "$elemMatch": {
            "sub_total": {
                "$lt": 15
            }
        }
    }
})

# Krok 4: Zakoncz pomiar czasu.
end_time = time.time()

# Krok 5: Przywróć usunięte dokumenty z kopii.
for doc in documents_to_delete:
    doc_id = doc.pop('_id', None)
    collection.insert_one(doc)

# Podsumowanie
execution_time_ms = (end_time - start_time) * 1000  # Przeliczanie na milisekundy
print(f"Czas wykonania usuwania danych: {execution_time_ms:.2f} milisekund")
print(f"Liczba usuniętych dokumentów (a następnie przywróconych): {result.deleted_count}")
