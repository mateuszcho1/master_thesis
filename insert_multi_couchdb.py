import requests
import json
import time
import datetime

# Dane do logowania
login = 'admin'
password = 'haslo'

# Nazwa bazy danych
db_name = 'multi_1_mln_db'

# Adres serwera CouchDB
couchdb_url = f'http://127.0.0.1:5984/{db_name}/'

# Tworzymy sesję z autoryzacją
session = requests.Session()
session.auth = (login, password)

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
response = session.post(couchdb_url, json=client_data)
response_data = response.json()

# Sprawdzanie, czy odpowiedź zawiera id dokumentu
if 'id' in response_data:
    document_id = response_data['id']
else:
    raise Exception("Błąd wstawiania dokumentu")

end_time = time.time()

# Usunięcie dodanego dokumentu
if 'rev' in response_data:
    rev = response_data['rev']
    session.delete(f"{couchdb_url}{document_id}?rev={rev}")

# Wyświetlenie wyniku
execution_time_ms = (end_time - start_time) * 1000  # Przeliczanie na milisekundy
print(f"Czas wykonania dodawania i usuwania dokumentu: {execution_time_ms:.2f} milisekund")
