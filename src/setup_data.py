import sqlite3
import pandas as pd
import numpy as np
from faker import Faker
import random
from datetime import datetime, timedelta

# Configuracion inicial
fake = Faker()
conn = sqlite3.connect('data/inventario_pro.db')
cursor = conn.cursor()

def create_tables():
    cursor.execute('''CREATE TABLE IF NOT EXISTS dim_productos (
                   producto_id INTEGER PRIMARY KEY,
                   nombre TEXT,
                   categoria TEXT,
                   costo_unitario REAL,
                   lead_time INTEGER)''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS fact_ventas (
        venta_id INTEGER PRIMARY KEY AUTOINCREMENT,
        fecha DATE,
        producto_id INTEGER,
        cantidad_vendida INTEGER,
        FOREIGN KEY (producto_id) REFERENCES dim_productos (producto_id))''')
    conn.commit()

def generate_synthetic_data():
    # 1 Creacion de productos
    categorias = ['Hilos', 'Tintes', 'Accesorios', 'Telas']
    productos = []
    for i in range(1,21): # 20 productos distintos
        productos.append((
            i,
            f'{random.choice(categorias)} - Tipo {fake.word().capitalize()}',
            random.choice(categorias),
            round(random.uniform(10.5, 100.0),2),
            random.randint(3,15) # Dias que demora el proveedor
        ))
    cursor.executemany('INSERT INTO dim_productos VALUES (?,?,?,?,?)', productos) 


    # 2 Crear las ventas estacionales (ultimos 12 meses)
    ventas = []
    fecha_inicio = datetime.now() - timedelta(days=365)

    for _ in range(2000): # 2000 registros de ventas
        fecha_venta = fecha_inicio + timedelta(days=random.randint(0,364))
        # Simulamos que los fines de semana se vende mas
        multiplicador = 2 if fecha_venta.weekday() >= 5 else 1

        ventas.append((
            fecha_venta.strftime('%Y-%m-%d'),
            random.randint(1,20),
            random.randint(1,10) * multiplicador
        ))
    cursor.executemany('INSERT INTO fact_ventas (fecha, producto_id, cantidad_vendida) VALUES (?,?,?)', ventas)
    conn.commit()


if __name__ == '__main__':
    print('Creando tablas...')
    create_tables()
    print('Generando datos sinteticos (tardara uno segundos)')
    generate_synthetic_data()
    print('Listo base de datos en data/inventario_pro.db creada')




  
      
