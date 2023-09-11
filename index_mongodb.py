from pymongo import MongoClient, ASCENDING

# Utworzenie połączenia
client = MongoClient('mongodb://localhost:27017/')

# Wybór bazy danych
db = client['Test_10mln_db']

# Wybór kolekcji
collection = db['Client']

# Tworzenie indeksu
# collection.create_index([("country", ASCENDING), ("age", ASCENDING)])
collection.create_index("client_id")

# Zamknięcie połączenia
client.close()
