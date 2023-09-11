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

# Przygotowanie zapytania z operacją agregacji
pipeline = [
    {
        "$unwind": "$addresses"
    },
    {
        "$unwind": "$orders"
    },
    {
        "$unwind": "$orders.details"
    },
    {
        "$match": {
            "orders.details.sub_total": {"$gte": 100, "$lte": 200}
        }
    },
    {
        "$project": {
            "_id": 0,
            "id_client": "$_id",
            "client_name": "$name",
            "client_last_name": "$last_name",
            "street": "$addresses.street",
            "city": "$addresses.city",
            "state": "$addresses.state",
            "date_order": "$orders.date_order",
            "product_name": "$orders.details.product_name",
            "quantity": "$orders.details.quantity",
            "sub_total": "$orders.details.sub_total"
        }
    }
]

# Mierzenie czasu wykonania zapytania
start_time = time.time()
documents = list(collection.aggregate(pipeline))
end_time = time.time()

execution_time_ms = (end_time - start_time) * 1000  # Przeliczanie na milisekundy
print(f"Czas wykonania zapytania: {execution_time_ms:.2f} milisekund")
print(f"Liczba zwróconych dokumentów: {len(documents)}")

# Zamknięcie połączenia
client.close()
