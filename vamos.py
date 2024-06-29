import pandas as pd
from sqlalchemy import create_engine, VARCHAR, BIGINT, DECIMAL, INTEGER, DATETIME

# Función para importar el CSV a la base de datos
def importar_csv_a_mysql(archivo_csv, host, database, user, password, tabla_destino):
    # Definir los nombres de las columnas y tipos de datos esperados en MySQL
    columnas_mysql = {
        'id': BIGINT,
        'quantity': INTEGER,
        'price': DECIMAL(10, 2),
        'total_price': DECIMAL(10, 2),
        'created_at': DATETIME,
        'updated_at': DATETIME,
        'product_size_id': BIGINT,
        'order_id': BIGINT,
        'product_id': BIGINT,
        'quantity_sold': INTEGER,
        'code': VARCHAR(60),
        'quantity_decrease': INTEGER,
        'quantity_tasting': INTEGER,
        'sliced_quantity': INTEGER,
        'slices_quantity': INTEGER,
        'slices_quantity_sold': INTEGER,
        'refund_quantity': INTEGER,
        'slices_quantity_decreased': INTEGER,
        'product_order': INTEGER
    }
    
    # Cargar el archivo CSV en un DataFrame de pandas
    df = pd.read_csv(archivo_csv, skiprows=1, names=list(columnas_mysql.keys()), dtype=str)
    
    # Crear una conexión a la base de datos MySQL usando sqlalchemy y mysql-connector-python
    if password == '':
        engine = create_engine(f'mysql+mysqlconnector://{user}@{host}/{database}')
    else:
        engine = create_engine(f'mysql+mysqlconnector://{user}:{password}@{host}/{database}')
    
    # Insertar los datos del DataFrame en la tabla MySQL
    try:
        with engine.connect() as conn:
            # Insertar los datos en lotes pequeños para evitar problemas de tamaño de lote
            chunksize = 1000  # Puedes ajustar este valor según sea necesario
            for i in range(0, len(df), chunksize):
                df_chunk = df[i:i + chunksize]
                df_chunk.to_sql(tabla_destino, con=conn, if_exists='append', index=False, dtype=columnas_mysql)
                
        print(f"¡CSV importado exitosamente a la tabla '{tabla_destino}' en la base de datos '{database}'!")
    except Exception as e:
        print(f"Error al importar CSV a MySQL: {str(e)}")
    finally:
        engine.dispose()  # Cerrar la conexión

# Preguntar al usuario los detalles de conexión y archivo CSV a importar
def main():
    # Detalles de conexión a la base de datos MySQL
    host = 'localhost'
    database = input("Ingrese el nombre de la base de datos MySQL: ")
    user = 'root'
    password = ''  # Deja esto vacío si no hay contraseña
    
    # Ruta del archivo CSV
    archivo_csv = input("Ingrese la ruta completa del archivo CSV a importar: ")
    
    # Nombre de la tabla destino en la base de datos MySQL
    tabla_destino = input("Ingrese el nombre de la tabla en la que desea importar los datos: ")
    
    try:
        importar_csv_a_mysql(archivo_csv, host, database, user, password, tabla_destino)
    except Exception as e:
        print(f"Error al importar CSV a MySQL: {str(e)}")

if __name__ == "__main__":
    main()
