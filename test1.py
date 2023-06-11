#!/usr/bin/env python3
import psycopg2
import pandas as pd

# Establecer la conexión con la base de datos
conn = psycopg2.connect(database='data_acquisition', user="acquisition", password='0PZ9TVXV', host="localhost", port="5432")
cursor = conn.cursor()

# Consulta SQL para obtener los datos
query = '''
SELECT h.date_price, a.symbol, h.close_price
FROM assets a
INNER JOIN half_hours h ON a.coin_id = h.coin_id
'''

# Ejecutar la consulta
cursor.execute(query)

# Obtener los resultados de la consulta
results = cursor.fetchall()

# Cerrar la conexión con la base de datos
cursor.close()
conn.close()

# Construir el DataFrame
df = pd.DataFrame(results, columns=["date_price", "symbol", "close_price"])
df.to_csv('nombre_archivo.csv', index=False)


# Mostrar el DataFrame
print(df)