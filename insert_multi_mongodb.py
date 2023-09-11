from pymongo import MongoClient
import time
import datetime

# Parametry połączenia
mongodb_uri = 'mongodb://localhost:27017/'
database_name = 'multi_10_tys_db'
collection_name = 'Client'

# Utworzenie połączenia
client = MongoClient(mongodb_uri)

# Wybór bazy danych
db = client[database_name]

# Wybór kolekcji
collection = db[collection_name]

# Przygotowanie danych
client_data = {
    "name": "John",
    "last_name": "Doe",
    "nickname": "JohnD123",
    "email": "john.doe@example.com",
    "num_tel": "123-456-7890",
    "age": 30,
    "addresses": [
        {
            "street": "123 Main St",
            "city": "CityName",
            "state": "StateName",
            "zip_code": "12345"
        }
    ],
    "orders": [
        {
            "date_order": datetime.datetime.utcnow().isoformat(),
            "total_price": 300.50,
            "details": [
                {
                    "quantity": 2,
                    "sub_total": 100.20,
                    "product_name": "Product1"
                },
                {
                    "quantity": 1,
                    "sub_total": 100.15,
                    "product_name": "Product2"
                },
                {
                    "quantity": 1,
                    "sub_total": 100.15,
                    "product_name": "Product3"
                }
            ]
        }
    ]
}

# Pomiar czasu i wstawianie danych
start_time = time.time()

# Wstawianie danych
inserted = collection.insert_one(client_data)

end_time = time.time()

# Usunięcie dodanego dokumentu
collection.delete_one({'_id': inserted.inserted_id})

# Wyświetlenie wyniku
execution_time_ms = (end_time - start_time) * 1000  # Przeliczanie na milisekundy
print(f"Czas wykonania dodawania i usuwania dokumentu: {execution_time_ms:.2f} milisekund")
