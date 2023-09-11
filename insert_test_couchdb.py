import requests
import time
import json

# Dane do logowania
login = 'admin'
password = 'haslo'

# Nazwa bazy danych
db_name = 'test1mlndb' 

# Adres serwera CouchDB
couchdb_url = 'http://127.0.0.1:5984/'

# Tworzymy sesję z autoryzacją
session = requests.Session()
session.auth = (login, password)

# Przygotuj dane do wstawienia
new_client = {
    "id_client": 10000001,
    "name": "name",
    "last_name": "surname",
    "nickname": "namesurname",
    "email": "name.surname@gmail.com",
    "country": "Poland",
    "num_tel": "123-456-789",
    "age": 26
}

# Rozpocznij pomiar czasu
start_time = time.time()

# Wykonanie zapytania INSERT
r = session.post(couchdb_url + db_name, json=new_client)

# Zakończenie pomiaru czasu
end_time = time.time()

# Obliczenie różnicy czasu w milisekundach
execution_time = (end_time - start_time) * 1000

if r.status_code == 201:
    response = r.json()
    print(f"Czas wykonania zapytania: {execution_time} ms")
    print(f"ID dodanego dokumentu: {response['id']}")
    
    # Usunięcie dodanego dokumentu
    delete_response = session.delete(couchdb_url + db_name + '/' + response['id'] + '?rev=' + response['rev'])
    
    if delete_response.status_code == 200:
        print(f"Dokument o ID: {response['id']} został pomyślnie usunięty.")
    else:
        print(f"Błąd podczas usuwania dokumentu: {delete_response.status_code}, {delete_response.json()}")

else:
    print(f"Błąd podczas dodawania dokumentu: {r.status_code}, {r.json()}")
