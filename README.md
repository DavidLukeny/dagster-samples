# Proyecto Dagster - PostgreSQL Stats & Spark Jobs en K3s

Este proyecto contiene múltiples pipelines de Dagster:
1. **PostgreSQL Stats**: Asset que obtiene estadísticas de tablas PostgreSQL
2. **Spark Jobs en K3s**: Jobs de Apache Spark ejecutados en cluster K3s con schedules (cron)

---

## 🐘 PostgreSQL Stats

### Configuración

La conexión a PostgreSQL está configurada en el archivo `postgresql_tables_stats.py`:

```python
DB_CONFIG = {
    "host": "192.168.0.27",
    "port": 30032,
    "database": "generic",
    "user": "dagster",
    "password": "dagster123"
}
```

## Ejecución

### Opción 1: Interfaz Web de Dagster (Dagit)

```bash
dagster dev -f postgresql_tables_stats.py
```

Luego abre tu navegador en `http://localhost:3000` y materializa el asset `postgresql_tables_statistics`.

### Opción 2: Ejecución directa desde línea de comandos

```bash
dagster asset materialize -f postgresql_tables_stats.py -a postgresql_tables_statistics
```

## Estadísticas que se obtienen

Para cada tabla en la base de datos, el script obtiene:

- **Nombre de la tabla**
- **Cantidad de registros**: Número total de filas
- **Tamaño total**: Espacio en disco (tabla + índices)
- **Tamaño de tabla**: Espacio que ocupa solo la tabla
- **Tamaño de índices**: Espacio que ocupan los índices
- **Cantidad de columnas**: Número de columnas en la tabla
- **Lista de columnas**: Nombres de todas las columnas
- **Llaves primarias**: Columnas que forman la llave primaria
- **Llaves foráneas**: Referencias a otras tablas
- **Índices**: Lista de índices definidos en la tabla

## Salida

El asset retorna un `pandas.DataFrame` con todas las estadísticas y también muestra un resumen detallado en los logs de Dagster.

## Estructura del código

- `get_connection()`: Establece conexión a PostgreSQL
- `get_all_tables()`: Lista todas las tablas del esquema public
- `get_table_row_count()`: Cuenta registros de una tabla
- `get_table_size()`: Obtiene tamaño en disco
- `get_table_columns()`: Lista columnas y sus tipos
- `get_primary_keys()`: Identifica llaves primarias
- `get_foreign_keys()`: Identifica llaves foráneas
- `get_indexes()`: Lista índices de la tabla
- `postgresql_tables_statistics`: Asset principal que coordina todo

---

## ⚡ Spark Jobs con Schedules

### Archivos disponibles

#### 1. `spark_job_scheduled.py` - Ejemplo simple

Un job básico que ejecuta el ejemplo SparkPi de Spark:
- **Asset**: `execute_spark_pi_job`
- **Job**: `spark_pi_job`
- **Schedule**: Diario a las 2:00 AM (`0 2 * * *`)

#### 2. `spark_jobs_multi.py` - Múltiples ejemplos

Incluye varios ejemplos de Spark con diferentes schedules:

| Ejemplo | Job | Schedule | Descripción |
|---------|-----|----------|-------------|
| SparkPi | `spark_pi_job` | Diario 2:00 AM | Calcula Pi usando Monte Carlo |
| WordCount | `spark_wordcount_job` | Cada 6 horas | Cuenta palabras en texto |
| PageRank | `spark_pagerank_job` | Lunes 3:00 AM | Algoritmo de ranking |
| ML (LogisticRegression) | `spark_ml_job` | Primer día del mes 4:00 AM | Machine Learning |

#### 3. `custom_spark_job.py` + `custom_spark_script.py` - Scripts personalizados

Ejecuta scripts PySpark personalizados:
- **Script**: `custom_spark_script.py` - Script PySpark de ejemplo
- **Assets**: `run_custom_spark_script`, `run_custom_spark_with_data_processing`
- **Jobs**: `custom_spark_job`, `data_processing_spark_job`
- **Schedules**: 
  - Cada hora (`custom_spark_hourly`)
  - Cada 30 minutos (`data_processing_30min`)
  - Diario 6:00 AM (`custom_spark_daily`)

Este ejemplo muestra cómo ejecutar tus propios scripts PySpark con Dagster.

### Ejecución de Spark Jobs

#### Opción 1: Interfaz Web con Schedules activos

```bash
# Iniciar Dagster con schedules habilitados
dagster dev -f spark_jobs_multi.py
```

Esto iniciará:
- La interfaz web en `http://localhost:3000`
- El daemon de schedules (ejecuta los cron automáticamente)

En la interfaz web:
1. Ve a "Assets" y materializa manualmente cualquier asset
2. Ve a "Overview" → "Schedules" para ver todos los schedules
3. Activa/desactiva schedules según necesites
4. Ve "Runs" para ver el historial de ejecuciones

#### Opción 2: Ejecución manual desde línea de comandos

```bash
# Ejecutar un job específico
dagster job execute -f spark_jobs_multi.py -j spark_pi_job

# Ejecutar con el archivo simple
dagster job execute -f spark_job_scheduled.py -j spark_pi_job

# Materializar un asset específico
dagster asset materialize -f spark_jobs_multi.py -a spark_pi_example
```

#### Opción 3: Solo el daemon de schedules (sin interfaz web)

```bash
# Iniciar solo el daemon para que los schedules se ejecuten automáticamente
dagster-daemon run
```

### Personalizar Schedules (Cron)

Ejemplos de expresiones cron:

```python
"0 2 * * *"      # Cada día a las 2:00 AM
"*/15 * * * *"   # Cada 15 minutos
"0 */2 * * *"    # Cada 2 horas
"0 9 * * 1-5"    # Lunes a Viernes a las 9:00 AM
"0 0 * * 0"      # Cada domingo a medianoche
"0 12 1 * *"     # Primer día de cada mes al mediodía
"0 0 1 1 *"      # Primer día del año
```

Formato: `minuto hora día_mes mes día_semana`

### Ver logs de ejecución

Los logs detallados de cada ejecución de Spark se muestran en:
- La interfaz web de Dagster (pestaña "Compute logs" en cada run)
- La salida del daemon si ejecutas `dagster-daemon run`
- Stdout/stderr capturados en el resultado del asset

---

## 📚 Resumen de archivos

| Archivo | Descripción | Tipo |
|---------|-------------|------|
| `postgresql_tables_stats.py` | Estadísticas de tablas PostgreSQL | Asset |
| `spark_job_scheduled.py` | Job simple de Spark en K3s con schedule | Job + Schedule |
| `spark_jobs_multi.py` | Múltiples jobs de Spark en K3s con schedules | Jobs + Schedules |
| `custom_spark_job.py` | Ejecutor de scripts PySpark personalizados en K3s | Job + Schedule |

---
