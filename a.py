HOLA MAESTRA. 29/06/24 haydee


import pymysql
import pandas as pd

def conectar_a_bd(host, user, password):
    # Conexión a la base de datos MySQL
    conexion = pymysql.connect(
        host=host,
        user=user,
        password=password
    )
    return conexion

def obtener_bases_de_datos(conexion):
    # Obtener lista de bases de datos
    with conexion.cursor() as cursor:
        cursor.execute("SHOW DATABASES")
        bases_de_datos = cursor.fetchall()
    return [bd[0] for bd in bases_de_datos]

def obtener_tablas(conexion, base_de_datos):
    # Obtener lista de tablas de una base de datos específica
    with conexion.cursor() as cursor:
        cursor.execute(f"USE {base_de_datos}")
        cursor.execute("SHOW TABLES")
        tablas = cursor.fetchall()
    return [tabla[0] for tabla in tablas]

def cargar_csv_en_tabla(conexion, tabla, csv_path):
    # Leer CSV con low_memory=False para evitar advertencias de tipos de datos mixtos
    df = pd.read_csv(csv_path, low_memory=False)

    # Rellenar valores faltantes con None
    df = df.where(pd.notnull(df), None)

    # Obtener columnas de la tabla
    with conexion.cursor() as cursor:
        cursor.execute(f"DESCRIBE {tabla}")
        columnas_tabla = cursor.fetchall()
    columnas_tabla = [columna[0] for columna in columnas_tabla]

    # Verificar que las columnas del CSV coincidan con las de la tabla
    columnas_csv = df.columns.tolist()
    print("Columnas del CSV:", columnas_csv)
    print("Columnas de la tabla:", columnas_tabla)
    
    if set(columnas_csv) != set(columnas_tabla):
        raise ValueError("Las columnas del CSV no coinciden con las de la tabla.")

    # Insertar datos en la tabla por partes
    chunk_size = 10000  # Número de filas por cada inserción
    for start in range(0, len(df), chunk_size):
        end = start + chunk_size
        chunk = df.iloc[start:end]

        with conexion.cursor() as cursor:
            for _, row in chunk.iterrows():
                valores = tuple(row[columnas_tabla])
                query = f"INSERT INTO {tabla} ({', '.join(columnas_tabla)}) VALUES ({', '.join(['%s'] * len(valores))})"
                cursor.execute(query, valores)
        conexion.commit()

def main():
    host = 'localhost'
    user = 'root'
    password = ''

    conexion = conectar_a_bd(host, user, password)
    bases_de_datos = obtener_bases_de_datos(conexion)
    
    print("Seleccione la base de datos:")
    for i, bd in enumerate(bases_de_datos):
        print(f"{i + 1}. {bd}")
    bd_index = int(input("Ingrese el número de la base de datos: ")) - 1
    database = bases_de_datos[bd_index]

    tablas = obtener_tablas(conexion, database)
    print(f"Tablas en la base de datos {database}:")
    for i, tabla in enumerate(tablas):
        print(f"{i + 1}. {tabla}")
    tabla_index = int(input("Ingrese el número de la tabla: ")) - 1
    tabla = tablas[tabla_index]

    csv_path = input("Ingrese la ruta del archivo CSV: ")

    try:
        cargar_csv_en_tabla(conexion, tabla, csv_path)
        print(f"Datos insertados correctamente en la tabla {tabla}.")
    except ValueError as e:
        print(e)
    finally:
        conexion.close()

if __name__ == "__main__":
    main()
