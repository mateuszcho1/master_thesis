# import requests
# import json

# # Dane do logowania
# login = 'admin'
# password = 'haslo'

# # Nazwa bazy danych
# db_name = 'test10mlndb' 

# # Adres serwera CouchDB
# couchdb_url = 'http://127.0.0.1:5984/'

# # Tworzymy sesję z autoryzacją
# session = requests.Session()
# session.auth = (login, password)

# # Tworzymy indeks na kolumnie 'country'
# index_country = {
#     "index": {
#         "fields": ["country"]
#     },
#     "name": "country-index",
#     "type": "json"
# }

# r = session.post(couchdb_url + db_name + '/_index', json=index_country)
# if r.status_code != 200:
#     print('Błąd podczas tworzenia indeksu:', r.json())

# # Tworzymy indeks na kolumnie 'age'
# index_age = {
#     "index": {
#         "fields": ["age"]
#     },
#     "name": "age-index",
#     "type": "json"
# }

# r = session.post(couchdb_url + db_name + '/_index', json=index_age)
# if r.status_code != 200:
#     print('Błąd podczas tworzenia indeksu:', r.json())

import requests

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

# Wykonanie zapytania do utworzenia indeksu
r = session.post(couchdb_url + db_name + '/_index', json={
    "index": {
        "fields": ["country", "age"]
    },
    "name": "country-age-index",
    "type": "json"
})

if r.status_code == 200:
    print("Indeks został pomyślnie utworzony.")
else:
    print(f"Błąd podczas tworzenia indeksu: {r.status_code}, {r.json()}")

