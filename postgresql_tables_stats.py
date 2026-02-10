"""
Dagster asset para obtener estadísticas de todas las tablas en PostgreSQL
"""
from dagster import asset, OpExecutionContext
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from typing import Dict, List


# Configuración de conexión
DB_CONFIG = {
    "host": "192.168.0.27",
    "port": 30032,
    "database": "generic",
    "user": "dagster",
    "password": "dagster123"
}


def get_connection():
    """Crear conexión a PostgreSQL"""
    return psycopg2.connect(**DB_CONFIG)


def get_all_tables(cursor) -> List[str]:
    """Obtener lista de todas las tablas en el esquema public"""
    query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
    """
    cursor.execute(query)
    return [row['table_name'] for row in cursor.fetchall()]


def get_table_row_count(cursor, table_name: str) -> int:
    """Obtener cantidad de registros en una tabla"""
    query = f'SELECT COUNT(*) as count FROM "{table_name}";'
    cursor.execute(query)
    return cursor.fetchone()['count']


def get_table_size(cursor, table_name: str) -> Dict:
    """Obtener tamaño de la tabla en disco"""
    query = f"""
        SELECT 
            pg_size_pretty(pg_total_relation_size('"{table_name}"')) as total_size,
            pg_size_pretty(pg_relation_size('"{table_name}"')) as table_size,
            pg_size_pretty(pg_indexes_size('"{table_name}"')) as indexes_size
    """
    cursor.execute(query)
    return cursor.fetchone()


def get_table_columns(cursor, table_name: str) -> List[Dict]:
    """Obtener información de columnas"""
    query = """
        SELECT 
            column_name,
            data_type,
            is_nullable,
            column_default
        FROM information_schema.columns
        WHERE table_schema = 'public' 
        AND table_name = %s
        ORDER BY ordinal_position;
    """
    cursor.execute(query, (table_name,))
    return cursor.fetchall()


def get_primary_keys(cursor, table_name: str) -> List[str]:
    """Obtener llaves primarias de la tabla"""
    query = """
        SELECT a.attname as column_name
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = %s::regclass
        AND i.indisprimary;
    """
    cursor.execute(query, (table_name,))
    return [row['column_name'] for row in cursor.fetchall()]


def get_foreign_keys(cursor, table_name: str) -> List[Dict]:
    """Obtener llaves foráneas de la tabla"""
    query = """
        SELECT
            kcu.column_name,
            ccu.table_name AS foreign_table_name,
            ccu.column_name AS foreign_column_name
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
        WHERE tc.constraint_type = 'FOREIGN KEY'
        AND tc.table_name = %s;
    """
    cursor.execute(query, (table_name,))
    return cursor.fetchall()


def get_indexes(cursor, table_name: str) -> List[Dict]:
    """Obtener índices de la tabla"""
    query = """
        SELECT
            indexname as index_name,
            indexdef as index_definition
        FROM pg_indexes
        WHERE schemaname = 'public'
        AND tablename = %s;
    """
    cursor.execute(query, (table_name,))
    return cursor.fetchall()


@asset
def postgresql_tables_statistics(context: OpExecutionContext) -> pd.DataFrame:
    """
    Asset que obtiene estadísticas completas de todas las tablas en PostgreSQL
    
    Returns:
        DataFrame con estadísticas de cada tabla
    """
    conn = None
    try:
        # Conectar a la base de datos
        context.log.info(f"Conectando a PostgreSQL en {DB_CONFIG['host']}:{DB_CONFIG['port']}")
        conn = get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Obtener lista de tablas
        tables = get_all_tables(cursor)
        context.log.info(f"Encontradas {len(tables)} tablas")
        
        # Recopilar estadísticas para cada tabla
        statistics = []
        
        for table_name in tables:
            context.log.info(f"Procesando tabla: {table_name}")
            
            try:
                # Obtener estadísticas
                row_count = get_table_row_count(cursor, table_name)
                size_info = get_table_size(cursor, table_name)
                columns = get_table_columns(cursor, table_name)
                primary_keys = get_primary_keys(cursor, table_name)
                foreign_keys = get_foreign_keys(cursor, table_name)
                indexes = get_indexes(cursor, table_name)
                
                # Compilar información
                table_stats = {
                    'tabla': table_name,
                    'registros': row_count,
                    'tamaño_total': size_info['total_size'],
                    'tamaño_tabla': size_info['table_size'],
                    'tamaño_indices': size_info['indexes_size'],
                    'cantidad_columnas': len(columns),
                    'columnas': ', '.join([col['column_name'] for col in columns]),
                    'llaves_primarias': ', '.join(primary_keys) if primary_keys else 'Ninguna',
                    'cantidad_llaves_foraneas': len(foreign_keys),
                    'llaves_foraneas': ', '.join([f"{fk['column_name']} -> {fk['foreign_table_name']}.{fk['foreign_column_name']}" for fk in foreign_keys]) if foreign_keys else 'Ninguna',
                    'cantidad_indices': len(indexes),
                    'indices': ', '.join([idx['index_name'] for idx in indexes]) if indexes else 'Ninguno'
                }
                
                statistics.append(table_stats)
                
            except Exception as e:
                context.log.error(f"Error procesando tabla {table_name}: {str(e)}")
                continue
        
        # Crear DataFrame con resultados
        df = pd.DataFrame(statistics)
        
        # Mostrar resumen en logs
        context.log.info("\n" + "="*100)
        context.log.info("RESUMEN DE ESTADÍSTICAS DE TABLAS")
        context.log.info("="*100)
        context.log.info(f"\nTotal de tablas procesadas: {len(df)}")
        context.log.info(f"\nTablas con más registros:")
        context.log.info(df.nlargest(5, 'registros')[['tabla', 'registros', 'tamaño_total']].to_string())
        
        # Imprimir tabla completa
        context.log.info("\n" + "="*100)
        context.log.info("DETALLE COMPLETO DE TODAS LAS TABLAS")
        context.log.info("="*100)
        for _, row in df.iterrows():
            context.log.info(f"\n📊 Tabla: {row['tabla']}")
            context.log.info(f"   • Registros: {row['registros']:,}")
            context.log.info(f"   • Tamaño total: {row['tamaño_total']}")
            context.log.info(f"   • Tamaño tabla: {row['tamaño_tabla']}")
            context.log.info(f"   • Tamaño índices: {row['tamaño_indices']}")
            context.log.info(f"   • Columnas ({row['cantidad_columnas']}): {row['columnas']}")
            context.log.info(f"   • Llaves primarias: {row['llaves_primarias']}")
            context.log.info(f"   • Llaves foráneas ({row['cantidad_llaves_foraneas']}): {row['llaves_foraneas']}")
            context.log.info(f"   • Índices ({row['cantidad_indices']}): {row['indices']}")
        
        return df
        
    except Exception as e:
        context.log.error(f"Error conectando a la base de datos: {str(e)}")
        raise
    
    finally:
        if conn:
            conn.close()
            context.log.info("Conexión cerrada")


# Definición del repositorio de Dagster
from dagster import Definitions

defs = Definitions(
    assets=[postgresql_tables_statistics]
)
