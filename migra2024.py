import mysql.connector
from mysql.connector import errorcode

def migrate_data():
    try:
        cnx = mysql.connector.connect(user='root', password='',
                                      host='localhost', database='base_brandon')
        cursor = cnx.cursor()

        cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'base_brandon'")
        tables = cursor.fetchall()

        for (table_name,) in tables:
            cursor.execute(f"""
                SELECT COUNT(*) 
                FROM information_schema.columns
                WHERE table_schema = 'base_brandon' 
                AND table_name = '{table_name}'
                AND column_name = 'update_at'
            """)
            (has_update_at_column,) = cursor.fetchone()

            if has_update_at_column > 0:
                cursor.execute(f"""
                    SELECT COUNT(*)
                    FROM information_schema.tables
                    WHERE table_schema = 'nueva_base'
                    AND table_name = '{table_name}'
                """)
                (table_exists,) = cursor.fetchone()

                if table_exists == 0:
                    cursor.execute(f"CREATE TABLE nueva_base.{table_name} LIKE base_brandon.{table_name}")

                cursor.execute(f"""
                    SELECT COUNT(*)
                    FROM base_brandon.{table_name}
                    WHERE YEAR(update_at) = 2024
                """)
                (has_2024_data,) = cursor.fetchone()

                year = 2024 if has_2024_data > 0 else 2023

                cursor.execute(f"""
                    SELECT COUNT(*)
                    FROM base_brandon.{table_name}
                    WHERE YEAR(update_at) = {year}
                """)
                (total_records,) = cursor.fetchone()

                batch_size = 1000
                for offset in range(0, total_records, batch_size):
                    cursor.execute(f"""
                        INSERT IGNORE INTO nueva_base.{table_name}
                        SELECT * FROM base_brandon.{table_name}
                        WHERE YEAR(update_at) = {year}
                        LIMIT {batch_size} OFFSET {offset}
                    """)
                    cnx.commit()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Algo está mal con tu nombre de usuario o contraseña")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("La base de datos no existe")
        else:
            print(err)
    else:
        cursor.close()
        cnx.close()

if __name__ == "__main__":
    migrate_data()
