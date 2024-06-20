import pandas as pd

# Definir el diccionario de mapeo de nombres a IDs
mapeo = {
    'SIN DATOS': 1,
    'Chico': 2,
    'Individual': 3,
    'Grande': 4,
    'Unico': 5,
    'Mini': 6,
    '125 gramos': 7,
    '250 gramos': 8,
    '1 Kilo': 9,
    'Charola': 10,
    'caja': 11,
    '5 PZAS': 12,
    '2 PZAS': 13,
    '20': 14,
    'Panque Imperial 600 gr.': 15,
    'Mini Panque Imperial 100 gr.': 16,
    'Paq.': 17,
    '112 grs': 18,
    '450 grs': 19,
    '950 grs': 20,
    'Rebanada': 21,
    'Extra Chica': 22,
    'Chica': 23,
    'Mediana': 24,
    'Extra Grande': 25,
    '2 Extra Grande': 26,
    'Talla 36': 27,
    'Talla 38': 28,
    'Talla 40': 29,
    'Talla 42': 30,
    'Unitalla': 31,
    'Litro': 32,
    '10 personas': 33,
    'Mini Panque Imperial 120 gr.': 34,
    'Rebanadas': 35,
    'Otros': 36,
    'Ind': 37,
    'Frutos Rojos Grande': 38,
    'Zarzamora Chico': 39,
    'Frutos Rojos Chico': 40,
    'Zarzamora Individual': 41,
    'Frutos Rojos Individual': 42,
    'Zarzamora Grande': 43,
    'Dulce de Leche Chico': 44,
    'Dulce de leche Individual': 45,
    'Frutos Rojos Mini': 46,
    '50': 47,
    '10': 48,
    '40': 49,
    '7': 50,
    '1': 51,
    'Único': 52
}

# Ruta del archivo CSV
archivo_csv = r"C:\Users\brand\Downloads\order_invoice_products.csv"

# Cargar el archivo CSV en un DataFrame de pandas
df = pd.read_csv(archivo_csv)

# Aplicar el mapeo de IDs a la columna product_size_id
df['product_size_id'] = df['product_size_id'].map(mapeo)

# Guardar el DataFrame actualizado de vuelta al archivo CSV
df.to_csv(archivo_csv, index=False)

print("Proceso completado. El archivo CSV ha sido actualizado.")
