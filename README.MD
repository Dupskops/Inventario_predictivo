#  Sistema de optimización de inventario

Este proyecto desarrolla un motor de decisión automatizado para la gestión de inventarios, resolviendo el equilibrio crítico entre el **capital de trabajo** y el **nivel de servicio**. 



##  Problema de Negocio
Las empresas suelen enfrentar dos escenarios costosos:
1. **Stockout (Quiebre de Stock):** Pérdida de ventas y clientes por falta de producto.
2. **Overstock (Exceso de Inventario):** Capital inmovilizado y costos de almacenamiento innecesarios.

Este sistema utiliza **Inteligencia de Datos** para automatizar el "Cuándo" y el "Cuánto" comprar.

---

##  Stack Tecnológico
- **Lenguaje:** Python 
- **Librerías:** Pandas, Numpy, Faker (Datos Sintéticos)
- **Base de Datos:** SQLite (Esquema de Estrella)
- **Visualización:** Power BI (DAX dinámico)
- **Automatización:** GitHub Actions (Simulación de ETL)

---

##  Metodología y Lógica Científica

### 1. Ingeniería de Datos
Se diseñó una arquitectura relacional en SQLite con tablas de dimensiones (`dim_productos`) y hechos (`fact_ventas`). Se generó un corpus de **2,000 transacciones** con patrones de estacionalidad semanal.

### 2. Algoritmo de Optimización
El sistema no utiliza reglas fijas, sino fórmulas de ingeniería de suministros:

- **Stock de Seguridad ($SS$):** Protege contra la variabilidad de la demanda diaria ($\sigma_d$) y el tiempo de entrega ($L$).
  $$SS = Z \times \sigma_d \times \sqrt{L}$$
  *Donde $Z = 1.645$ para un nivel de servicio del 95%.*

- **Punto de Reorden ($ROP$):** El gatillo automático de compra.
  $$ROP = (\text{Demanda Promedio} \times L) + SS$$

### 3. Automatización (ETL)
Un script de Python (`inventory_optimizer.py`) procesa los datos crudos, calcula las métricas de suministro y exporta un reporte prescriptivo en CSV que alimenta el tablero de control.

---

##  Automatización y CI/CD (GitHub Actions)

Para asegurar que el reporte de inventarios sea siempre actual y accionable, este proyecto incluye un flujo de trabajo de **Integración y Despliegue Continuo (CI/CD)**:

- **Ejecución Automática:** El sistema está configurado para ejecutarse todos los lunes a las 00:00 UTC y tras cada actualización del código (`push`).
- **Pipeline de Datos:** 1. GitHub inicia una instancia virtual de Ubuntu.
  2. Se instala el entorno de Python y las dependencias necesarias.
  3. Se ejecuta el `inventory_optimizer.py`, procesando la lógica de suministro.
  4. Los resultados se guardan automáticamente en `data/reporte_inventario.csv`.
- **Resultado:** Power BI siempre consume datos frescos sin intervención humana, garantizando decisiones basadas en el estado real del almacén.

---
##  Dashboard de Control (Insights)
El reporte en Power BI permite:
- **Semáforo de Riesgo:** Identificación visual inmediata de productos críticos (Rojo/Amarillo/Verde).
- **Análisis de Dispersión:** Correlación entre el costo unitario y la volatilidad de la demanda.
- **Inversión Requerida:** Cálculo dinámico del presupuesto necesario para reposición por categoría.



---

##  Estructura del Proyecto
- `src/`: Scripts de creación de datos y motor de optimización.
- `data/`: Base de datos SQLite y reporte exportado.
- `notebooks/`: Análisis exploratorio de datos (EDA).
- `reports/`: Archivo .pbix de Power BI y capturas de pantalla.

---
# Clonar el proyecto
git clone https://github.com/Dupskops/Inventario_predictivo.git
cd inventario-pro-2026

# Crear y activar entorno virtual
python -m venv venv
# En Windows:
.\venv\Scripts\activate
# En Mac/Linux:
source venv/bin/activate

# Instalar dependencias
pip install -r requirements.txt

---

# Generar la base de datos
python src/setup_data.py

# Ejecutar el optimizador
python src/inventory_optimizer.py

# Visualización en Power BI
 - Abre el archivo reports/inventario_control.pbix con Power BI  Desktop.

 - Si los datos no cargan automáticamente, ve a Transformar datos > Configuración de origen de datos y apunta al archivo data/
 reporte_inventario.csv de tu carpeta local.

 - Haz clic en Actualizar y listo


**Autor:** Anibal Jahuar
