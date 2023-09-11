import pandas as pd
from pymongo import MongoClient

# Parametry połączenia
mongodb_uri = 'mongodb://localhost:27017/'
database_name = 'Test_100tys_db'
collection_name = 'Client'

# Utworzenie połączenia
client = MongoClient(mongodb_uri)

# Wybór bazy danych
db = client[database_name]

# Wybór kolekcji
collection = db[collection_name]

# Typy kolumn dla konwersji danych
column_types = {
    'id_client': int,
    'name': str,
    'last_name': str,
    'nickname': str,
    'email': str,
    'country': str,
    'num_tel': str,
    'age': int
}

# Wielkość partii do wczytania
chunksize = 5000

# Wczytanie i importowanie danych partiami
for chunk in pd.read_csv('export_100tys.csv', dtype=column_types, chunksize=chunksize):
    # Konwersja partii do formatu JSON
    data_json = chunk.to_dict('records')

    # Wstawienie partii do MongoDB
    collection.insert_many(data_json)

# Zamknięcie połączenia
client.close()
