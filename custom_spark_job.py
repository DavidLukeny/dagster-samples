"""
Dagster job que ejecuta un script PySpark personalizado con schedule
"""
from dagster import (
    asset,
    AssetExecutionContext,
    define_asset_job,
    ScheduleDefinition,
    Definitions,
)
import subprocess
import os
from datetime import datetime
from pathlib import Path


# Configuración de Spark para K3s
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "spark-submit")

# Configuración del cluster K3s
K8S_MASTER = os.environ.get("SPARK_K8S_MASTER", "k8s://https://127.0.0.1:6443")
SPARK_NAMESPACE = os.environ.get("SPARK_NAMESPACE", "default")
SPARK_SERVICE_ACCOUNT = os.environ.get("SPARK_SERVICE_ACCOUNT", "spark")
SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "apache/spark-py:latest")

# Ruta al script personalizado
SCRIPT_DIR = Path(__file__).parent
CUSTOM_SCRIPT = SCRIPT_DIR / "custom_spark_script.py"


@asset
def run_custom_spark_script(context: AssetExecutionContext) -> dict:
    """
    Ejecuta un script PySpark personalizado
    
    Este ejemplo demuestra cómo ejecutar cualquier script PySpark
    desde Dagster con parámetros personalizados.
    
    Returns:
        dict: Resultado de la ejecución
    """
    context.log.info("="*80)
    context.log.info("🚀 Ejecutando Custom PySpark Script en K3s")
    context.log.info("="*80)
    
    # Verificar que el script existe
    if not CUSTOM_SCRIPT.exists():
        raise FileNotFoundError(f"Script no encontrado: {CUSTOM_SCRIPT}")
    
    context.log.info(f"📄 Script: {CUSTOM_SCRIPT}")
    
    # Parámetros para el script
    num_rows = 1000  # Puedes parametrizar esto con config
    
    # Construir comando spark-submit para K3s
    command = [
        SPARK_SUBMIT,
        "--master", K8S_MASTER,
        "--deploy-mode", "cluster",
        "--name", "CustomSparkScript-Dagster",
        "--conf", f"spark.kubernetes.namespace={SPARK_NAMESPACE}",
        "--conf", f"spark.kubernetes.container.image={SPARK_IMAGE}",
        "--conf", f"spark.kubernetes.authenticate.driver.serviceAccountName={SPARK_SERVICE_ACCOUNT}",
        "--conf", "spark.executor.instances=2",
        "--conf", "spark.executor.memory=2g",
        "--conf", "spark.executor.cores=2",
        "--conf", "spark.driver.memory=2g",
        "--conf", "spark.sql.shuffle.partitions=10",
        "--conf", f"spark.kubernetes.file.upload.path=/tmp/spark-upload",
        str(CUSTOM_SCRIPT),
        str(num_rows)  # Parámetro para el script
    ]
    
    context.log.info(f"\n📝 Comando:")
    context.log.info(f"   {' '.join(command)}")
    context.log.info("")
    
    try:
        start_time = datetime.now()
        context.log.info(f"⏰ Inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Ejecutar el comando
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=600,  # 10 minutos
            cwd=str(SCRIPT_DIR)
        )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        context.log.info("")
        context.log.info("="*80)
        context.log.info("📊 RESULTADO")
        context.log.info("="*80)
        context.log.info(f"⏰ Finalización: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        context.log.info(f"⏱️  Duración: {duration:.2f} segundos")
        context.log.info(f"✅ Código de salida: {result.returncode}")
        
        # Mostrar stdout
        if result.stdout:
            context.log.info("\n📤 OUTPUT DEL SCRIPT:")
            context.log.info("-"*80)
            for line in result.stdout.split('\n'):
                if line.strip() and not line.startswith('24/') and not line.startswith('25/'):
                    # Filtrar líneas de logs de Spark para mostrar solo el output relevante
                    if any(x in line for x in ['🚀', '📊', '📋', '🔍', '📈', '✅', '🔢', '•']):
                        context.log.info(line)
            context.log.info("-"*80)
        
        # Verificar éxito
        if result.returncode == 0:
            context.log.info("\n✅ Script ejecutado exitosamente!")
            
            return {
                "status": "success",
                "script": str(CUSTOM_SCRIPT),
                "exit_code": result.returncode,
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "parameters": {"num_rows": num_rows}
            }
        else:
            context.log.error("\n❌ Error en la ejecución")
            context.log.error(f"STDERR:\n{result.stderr}")
            raise Exception(f"Script falló con código: {result.returncode}")
            
    except subprocess.TimeoutExpired:
        context.log.error("⏰ Timeout excedido")
        raise
    except Exception as e:
        context.log.error(f"❌ Error: {str(e)}")
        raise


@asset
def run_custom_spark_with_data_processing(context: AssetExecutionContext) -> dict:
    """
    Otro ejemplo que ejecuta un script Spark con procesamiento de datos más complejo
    
    Este puede leer datos de una fuente, procesarlos y escribir resultados.
    """
    context.log.info("🔄 Ejecutando job de procesamiento de datos con Spark")
    
    # Aquí puedes:
    # 1. Leer datos de una base de datos (usando el asset de PostgreSQL)
    # 2. Escribir datos temporales a archivos
    # 3. Ejecutar un script Spark que los procese
    # 4. Leer los resultados y retornarlos
    
    # Por ahora, solo ejecutamos el script básico
    return run_custom_spark_script.compute_fn(context)


# ============================================================================
# JOBS
# ============================================================================

custom_spark_job = define_asset_job(
    name="custom_spark_job",
    selection="run_custom_spark_script",
    description="Ejecuta script PySpark personalizado"
)

data_processing_job = define_asset_job(
    name="data_processing_spark_job",
    selection="run_custom_spark_with_data_processing",
    description="Job de procesamiento de datos con Spark"
)


# ============================================================================
# SCHEDULES
# ============================================================================

# Ejecutar el script personalizado cada hora
custom_spark_hourly_schedule = ScheduleDefinition(
    name="custom_spark_hourly",
    job=custom_spark_job,
    cron_schedule="0 * * * *",  # Cada hora en punto
    description="Ejecuta script personalizado cada hora"
)

# Ejecutar procesamiento de datos cada 30 minutos
data_processing_schedule = ScheduleDefinition(
    name="data_processing_30min",
    job=data_processing_job,
    cron_schedule="*/30 * * * *",  # Cada 30 minutos
    description="Procesamiento de datos cada 30 minutos"
)

# Ejecutar diariamente a las 6 AM
custom_spark_daily_schedule = ScheduleDefinition(
    name="custom_spark_daily",
    job=custom_spark_job,
    cron_schedule="0 6 * * *",  # Diario a las 6:00 AM
    description="Ejecuta script personalizado diariamente a las 6 AM"
)


# ============================================================================
# DEFINITIONS
# ============================================================================

defs = Definitions(
    assets=[
        run_custom_spark_script,
        run_custom_spark_with_data_processing,
    ],
    jobs=[
        custom_spark_job,
        data_processing_job,
    ],
    schedules=[
        custom_spark_hourly_schedule,
        data_processing_schedule,
        custom_spark_daily_schedule,
    ]
)
