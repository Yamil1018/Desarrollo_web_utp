import mysql.connector
import csv

# Conectar a la base de datos MySQL en XAMPP
conn = mysql.connector.connect(
    host='localhost',
    user='root',
    password='',  # Sin contraseña
    database='delitutti_sistema'
)

# Crear un cursor
cursor = conn.cursor()

# Ejecutar la consulta SQL para seleccionar solo las columnas 'id' y 'nombre' de la tabla 'product_sizes'
cursor.execute('SELECT id, name FROM product_sizes')

# Obtener todos los resultados de la consulta
resultados = cursor.fetchall()

# Definir el nombre del archivo CSV de respaldo
archivo_respaldo = 'respaldo_product_sizes.csv'

# Escribir los resultados en un archivo CSV
with open(archivo_respaldo, mode='w', newline='') as archivo:
    escritor_csv = csv.writer(archivo)
    # Escribir el encabezado
    escritor_csv.writerow(['id', 'name'])
    # Escribir las filas de datos
    escritor_csv.writerows(resultados)

# Cerrar la conexión
conn.close()

print(f"Respaldo completado. Los datos se han guardado en {archivo_respaldo}.")
