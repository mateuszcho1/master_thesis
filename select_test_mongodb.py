import time
from pymongo import MongoClient

# Utworzenie połączenia
client = MongoClient('mongodb://localhost:27017/')

# Wybór bazy danych
db = client['Test_1mln_db']

# Wybór kolekcji
collection = db['Client']

# ----------------------------------------------------------------------------

try:
    # Rozpocznij pomiar czasu
    start_time = time.time()

#==================================================================================================================

    # Wykonanie zapytania PROSTEGO SELECTA
    result = collection.find({"country": "Finland"}).batch_size(1000000)

    # # Wykonanie zapytania ZLOZONEGO SELECTA
    # result = collection.find({"country": "Estonia", "age": {"$gt": 35}}).sort("age", 1).batch_size(1000000)

#==================================================================================================================

    # Wymuś pobranie wyników
    result_list = list(result)

    # Zakończenie pomiaru czasu
    end_time = time.time()

    # Obliczenie różnicy czasu w milisekundach
    execution_time = (end_time - start_time) * 1000
    print(f"Czas wykonania zapytania: {execution_time} ms")
    print(f"Liczba wyników: {len(result_list)}")

    # Wydrukuj wyniki zapytania
    # for doc in result_list:
    #     print(doc)

except Exception as e:
    print(f"Wystąpił błąd: {e}")

finally:
    # Zamknięcie połączenia
    client.close()

# ----------------------------------------------------------------------------

