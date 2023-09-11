import time
from pymongo import MongoClient

# Utworzenie połączenia
client = MongoClient('mongodb://localhost:27017/')

# Wybór bazy danych
db = client['Test_100tys_db']

# Wybór kolekcji
collection = db['Client']

# ----------------------------------------------------------------------------

try:
    # Przygotuj dane do wstawienia
    new_client = {
        "id_client": 100001,
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
    result = collection.insert_one(new_client)

    # Zakończenie pomiaru czasu
    end_time = time.time()

    # Obliczenie różnicy czasu w milisekundach
    execution_time = (end_time - start_time) * 1000

    print(f"Czas wykonania zapytania: {execution_time} ms")
    print(f"ID dodanego dokumentu: {result.inserted_id}")

    # Usunięcie dodanego dokumentu
    collection.delete_one({"_id": result.inserted_id})

except Exception as e:
    print(f"Wystąpił błąd: {e}")

finally:
    # Zamknięcie połączenia
    client.close()
