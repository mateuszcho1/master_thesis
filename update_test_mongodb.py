from pymongo import MongoClient
import time

# Tworzenie połączenia
client = MongoClient('mongodb://localhost:27017/')

# Wybieranie bazy danych
db = client['Test_10mln_db']

# Wybieranie kolekcji
collection = db['Client']

# Określenie id dokumentu
document_id = 1115412

# Pobranie pierwotnej wartości "country" dla danego dokumentu
original_country = collection.find_one({'id_client': document_id})['country']

# Rozpoczęcie pomiaru czasu
start_time = time.time()

# Aktualizacja wartości "country" na "Poland"
collection.update_one({'id_client': document_id}, {'$set': {'country': 'Poland'}})

# Zakończenie pomiaru czasu
end_time = time.time()

# Obliczenie różnicy czasu w milisekundach
execution_time = (end_time - start_time) * 1000

print(f"Czas wykonania update: {execution_time} ms")

# Przywrócenie oryginalnej wartości "country"
collection.update_one({'id_client': document_id}, {'$set': {'country': original_country}})

