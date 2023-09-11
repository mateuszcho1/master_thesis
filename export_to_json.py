import pandas as pd
import pyodbc

# Parametry połączenia
server = 'DESKTOP-9F59L2S\SQLEXPRESS'
database = 'multi_1_mln_db'
driver = "{ODBC Driver 17 for SQL Server}"

# Tworzenie łańcucha połączenia
connection_string = f'DRIVER={driver};SERVER={server};DATABASE={database};Trusted_Connection=yes'

# Utworzenie połączenia
conn = pyodbc.connect(connection_string)

# Wczytywanie danych z bazy MS SQL do DataFrame'ów
clients_df = pd.read_sql("SELECT * FROM Client", conn)
addresses_df = pd.read_sql("SELECT * FROM Address", conn)
orders_df = pd.read_sql("SELECT * FROM CustomerOrder", conn)
order_details_df = pd.read_sql("SELECT * FROM OrderDetail", conn)
products_df = pd.read_sql("SELECT * FROM Product", conn)

# Zamknięcie połączenia
conn.close()

# Połączenie addresses_df z clients_df
clients_df['addresses'] = clients_df['id_client'].apply(lambda x: addresses_df[addresses_df['id_client'] == x].drop('id_client', axis=1).to_dict('records'))

# Połączenie orders_df z clients_df
clients_df['orders'] = clients_df['id_client'].apply(lambda x: orders_df[orders_df['id_client'] == x].drop('id_client', axis=1).to_dict('records'))

# Połączenie order_details_df z orders_df
def get_order_details(order_id):
    details = order_details_df[order_details_df['id_order'] == order_id].drop('id_order', axis=1)
    
    # Dodawanie nazwy produktu do szczegółów zamówienia
    details['product_name'] = details['id_product'].apply(lambda x: products_df[products_df['id_product'] == x]['product_name'].values[0])
    return details.drop('id_product', axis=1).to_dict('records')

for _, row in clients_df.iterrows():
    for order in row['orders']:
        order['details'] = get_order_details(order['id_order'])

# Usuwanie niepotrzebnych kolumn
clients_df.drop(['id_client'], axis=1, inplace=True)

# Eksport do pliku JSON
clients_df.to_json('output.json', orient='records', date_format='iso')