import csv
import os

def cargar_correspondencias(ruta_correspondencias):
    correspondencias = {}
    encodings = ['utf-8-sig', 'utf-8', 'latin-1']
    for encoding in encodings:
        try:
            with open(ruta_correspondencias, mode='r', newline='', encoding=encoding) as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    correspondencias[row['id']] = row['name']
            break  # Si hemos tenido éxito, salimos del bucle
        except UnicodeDecodeError:
            continue  # Si hay un error de decodificación, intentamos con el siguiente encoding

    return correspondencias

def reemplazar_ids_por_nombres(ruta_archivo_original, ruta_archivo_modificado, correspondencias):
    with open(ruta_archivo_original, mode='r', newline='', encoding='utf-8') as infile, \
         open(ruta_archivo_modificado + ".tmp", mode='w', newline='', encoding='utf-8') as outfile:
        
        reader = csv.DictReader(infile)
        fieldnames = reader.fieldnames
        
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        processed_rows = 0
        for row in reader:
            if 'product_size_id' in row:
                product_size_id = row['product_size_id']
                if product_size_id in correspondencias:
                    row['product_size_id'] = correspondencias[product_size_id]
            
            writer.writerow(row)
            processed_rows += 1
            
            if processed_rows % 100000 == 0:
                print(f"Filas procesadas: {processed_rows}")
        
        print("Proceso completo. Filas procesadas:", processed_rows)

    # Renombrar el archivo temporal al archivo final
    os.replace(ruta_archivo_modificado + ".tmp", ruta_archivo_modificado)

if __name__ == "__main__":
    ruta_correspondencias = r"C:\Users\brand\Downloads\respaldo_product_sizes.csv"
    ruta_archivo_modificar = r"C:\Users\brand\Downloads\order_invoice_products.csv"
    
    correspondencias = cargar_correspondencias(ruta_correspondencias)
    
    reemplazar_ids_por_nombres(ruta_archivo_modificar, ruta_archivo_modificar, correspondencias)
    
    print("Se ha completado el proceso de reemplazo.")
