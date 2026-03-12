import sqlite3
import pandas as pd
import numpy as np

def run_optimization():
    conn = sqlite3.connect('data/inventario_pro.db')
    
    # 1. Agregamos p.categoria a la consulta SQL
    query = '''
    SELECT v.fecha, v.cantidad_vendida, p.producto_id, p.nombre, p.categoria, p.lead_time, p.costo_unitario
    FROM fact_ventas v
    JOIN dim_productos p ON v.producto_id = p.producto_id
    '''
    df = pd.read_sql_query(query, conn)
    df['fecha'] = pd.to_datetime(df['fecha'])

    # 2. Agregamos 'categoria' al groupby para que no se pierda en el resumen
    stats = df.groupby(['producto_id', 'nombre', 'categoria', 'lead_time', 'costo_unitario']).agg(
        demanda_total=('cantidad_vendida', 'sum'),
        demanda_promedio=('cantidad_vendida', 'mean'),
        demanda_std=('cantidad_vendida', 'std')
    ).reset_index()

    # 3. Lógica de Inventario (Nivel de Servicio 95% -> Z = 1.645)
    Z = 1.645
    
    # Cálculo del Stock de Seguridad: SS = Z * sigma * sqrt(Lead_Time)
    stats['stock_seguridad'] = np.ceil(Z * stats['demanda_std'] * np.sqrt(stats['lead_time']))
    
    # Cálculo del Punto de Reorden: ROP = (Demanda_Promedio * Lead_Time) + Stock_Seguridad
    stats['punto_reorden'] = np.ceil((stats['demanda_promedio'] * stats['lead_time']) + stats['stock_seguridad'])

    # 4. Simulación de Stock Actual (Para fines del portafolio)
    # En la vida real esto vendría de una tabla de 'Inventario Actual'
    stats['stock_actual'] = stats['punto_reorden'] * np.random.uniform(0.5, 1.5, len(stats))
    stats['stock_actual'] = stats['stock_actual'].round(0)

    # 5. Generación de Alertas
    stats['necesita_pedido'] = stats['stock_actual'] <= stats['punto_reorden']
    stats['cantidad_a_pedir'] = np.where(stats['necesita_pedido'], stats['punto_reorden'] * 1.2, 0)

    # 6. Exportar resultado para Power BI
    stats.to_csv('data/reporte_inventario.csv', index=False)
    print("✅ Automatización completada. Reporte generado en 'data/reporte_inventario.csv'")
    conn.close()

if __name__ == "__main__":
    run_optimization()