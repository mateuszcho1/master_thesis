import time
from pymongo import MongoClient

# Utworzenie połączenia
client = MongoClient('mongodb://localhost:27017/')

# Wybór bazy danych
db = client['Test_10mln_db']

# Wybór kolekcji
collection = db['Client']

try:
    # Przygotowanie danych do usunięcia
    to_delete_docs = list(collection.find({"age": {"$gt": 60}, "country": "Poland"}))

    # Rozpocznij pomiar czasu
    start_time = time.time()

    # Wykonanie zapytania DELETE
    result = collection.delete_many({"age": {"$gt": 60}, "country": "Poland"})

    # Zakończenie pomiaru czasu
    end_time = time.time()

    # Obliczenie różnicy czasu w milisekundach
    execution_time = (end_time - start_time) * 1000

    print(f"Czas wykonania zapytania: {execution_time} ms")
    print(f"Liczba usuniętych dokumentów: {result.deleted_count}")

    # Dodanie usuniętych dokumentów z powrotem do bazy danych
    if to_delete_docs:
        collection.insert_many(to_delete_docs)

except Exception as e:
    print(f"Wystąpił błąd: {e}")

finally:
    # Zamknięcie połączenia
    client.close()
