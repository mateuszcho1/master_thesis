import requests
import time
import json

# Dane do logowania
login = 'admin'
password = 'haslo'

# Nazwa bazy danych
db_name = 'test100tysdb' 

# Adres serwera CouchDB
couchdb_url = 'http://127.0.0.1:5984/'

# Tworzymy sesję z autoryzacją
session = requests.Session()
session.auth = (login, password)

# Rozpocznij pomiar czasu
start_time = time.time()

# ========================================================================

# Wykonanie zapytania
r = session.post(couchdb_url + db_name + '/_find', json={
    "selector": {
        "country": "Finland"
    },
    "limit": 99999999  # Ustawienie wysokiego limitu
})


# # Wykonanie zapytania Zlozonego zapytania
# r = session.post(couchdb_url + db_name + '/_find', json={
#     "selector": {
#         "country": "Estonia",
#         "age": {"$gt": 35}
#     },
#     "sort": [{"age": "asc"}],
#     "limit": 99999999
# })

# ========================================================================

# Zakończenie pomiaru czasu
end_time = time.time()

# Obliczenie różnicy czasu w milisekundach
execution_time = (end_time - start_time) * 1000

if r.status_code == 200:
    response = r.json()
    if 'docs' in response:
        result_list = response['docs']
        print(f"Czas wykonania zapytania: {execution_time} ms")
        print(f"Liczba wyników: {len(result_list)}")
    else:
        print("Nie znaleziono żadnych dokumentów spełniających kryteria zapytania.")
        print("Odpowiedź serwera: ", response)
else:
    print(f"Błąd podczas wykonywania zapytania: {r.status_code}, {r.json()}")