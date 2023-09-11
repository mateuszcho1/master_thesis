import requests
import time
import json

# Dane do logowania
login = 'admin'
password = 'haslo'

# Nazwa bazy danych
db_name = 'test10mlndb' 

# Adres serwera CouchDB
couchdb_url = 'http://127.0.0.1:5984/'

# Tworzymy sesję z autoryzacją
session = requests.Session()
session.auth = (login, password)

# Określenie id dokumentu
document_id = '1115412'

# Pobranie pierwotnej wartości "country" oraz "_rev" dla danego dokumentu
doc = session.get(couchdb_url + db_name + '/' + document_id).json()
original_country = doc['country']
rev = doc['_rev']

# Aktualizacja wartości "country" na "Poland"
doc['country'] = 'Poland'

# Rozpoczęcie pomiaru czasu
start_time = time.time()

# Wysłanie zapytania aktualizującego
r = session.put(couchdb_url + db_name + '/' + document_id, json=doc)

# Zakończenie pomiaru czasu
end_time = time.time()

# Obliczenie różnicy czasu w milisekundach
execution_time = (end_time - start_time) * 1000

if r.status_code == 201:
    print(f"Czas wykonania update: {execution_time} ms")
else:
    print('Błąd podczas wykonywania zapytania:', r.json())

# Przywrócenie oryginalnej wartości "country"
doc['country'] = original_country
session.put(couchdb_url + db_name + '/' + document_id, json=doc)
