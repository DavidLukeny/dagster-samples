"""
Dagster jobs para ejecutar diferentes ejemplos de Spark con schedules configurables
"""
from dagster import (
    asset,
    AssetExecutionContext,
    define_asset_job,
    ScheduleDefinition,
    Definitions,
    Config,
)
import subprocess
import os
from datetime import datetime
from typing import Optional


# Configuración de Spark para K3s
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "spark-submit")

# Configuración del cluster K3s
# K3s por defecto usa el puerto 6443
K8S_MASTER = os.environ.get("SPARK_K8S_MASTER", "k8s://https://127.0.0.1:6443")
SPARK_NAMESPACE = os.environ.get("SPARK_NAMESPACE", "default")
SPARK_SERVICE_ACCOUNT = os.environ.get("SPARK_SERVICE_ACCOUNT", "spark")
SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "apache/spark:latest")

# JAR de ejemplos en el cluster
SPARK_EXAMPLES_JAR = os.environ.get("SPARK_EXAMPLES_JAR", "local:///opt/spark/examples/jars/spark-examples.jar")


class SparkJobConfig(Config):
    """Configuración para jobs de Spark en Kubernetes"""
    master: str = K8S_MASTER
    driver_memory: str = "1g"
    executor_memory: str = "1g"
    executor_cores: int = 1
    executor_instances: int = 2
    deploy_mode: str = "cluster"


def run_spark_example(
    context: AssetExecutionContext,
    class_name: str,
    job_name: str,
    args: list = None,
    config: SparkJobConfig = None
) -> dict:
    """
    Función auxiliar para ejecutar ejemplos de Spark
    
    Args:
        context: Contexto de ejecución de Dagster
        class_name: Nombre completo de la clase de Spark a ejecutar
        job_name: Nombre descriptivo del job
        args: Argumentos para pasar al job de Spark
        config: Configuración del job de Spark
    """
    if config is None:
        config = SparkJobConfig()
    
    if args is None:
        args = []
    
    context.log.info("="*80)
    context.log.info(f"🚀 Iniciando ejecución de Spark Job - {job_name}")
    context.log.info(f"☸️  Cluster K3s: {config.master}")
    context.log.info(f"📦 Namespace: {SPARK_NAMESPACE}")
    context.log.info("="*80)
    
    context.log.info(f"📦 Usando JAR: {SPARK_EXAMPLES_JAR}")
    
    # Construir comando spark-submit para Kubernetes
    command = [
        SPARK_SUBMIT,
        "--master", config.master,
        "--deploy-mode", config.deploy_mode,
        "--name", job_name,
        "--class", class_name,
        "--conf", f"spark.kubernetes.namespace={SPARK_NAMESPACE}",
        "--conf", f"spark.kubernetes.container.image={SPARK_IMAGE}",
        "--conf", f"spark.kubernetes.authenticate.driver.serviceAccountName={SPARK_SERVICE_ACCOUNT}",
        "--conf", f"spark.executor.instances={config.executor_instances}",
        "--conf", f"spark.executor.memory={config.executor_memory}",
        "--conf", f"spark.executor.cores={config.executor_cores}",
        "--conf", f"spark.driver.memory={config.driver_memory}",
        SPARK_EXAMPLES_JAR,
    ] + args
    
    context.log.info(f"📝 Comando a ejecutar:")
    context.log.info(f"   {' '.join(command)}")
    context.log.info("")
    
    try:
        start_time = datetime.now()
        context.log.info(f"⏰ Hora de inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=600  # timeout de 10 minutos
        )
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        context.log.info("")
        context.log.info("="*80)
        context.log.info("📊 RESULTADO DE LA EJECUCIÓN")
        context.log.info("="*80)
        context.log.info(f"⏰ Hora de finalización: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        context.log.info(f"⏱️  Duración: {duration:.2f} segundos")
        context.log.info(f"✅ Código de salida: {result.returncode}")
        
        # Mostrar líneas clave del output
        if result.stdout:
            context.log.info("")
            context.log.info("📤 OUTPUT:")
            for line in result.stdout.split('\n')[-20:]:  # Últimas 20 líneas
                if line.strip():
                    context.log.info(line)
        
        if result.returncode == 0:
            context.log.info("")
            context.log.info(f"✅ {job_name} ejecutado exitosamente!")
            
            return {
                "status": "success",
                "job_name": job_name,
                "exit_code": result.returncode,
                "duration_seconds": duration,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
            }
        else:
            context.log.error(f"❌ Error en la ejecución de {job_name}")
            raise Exception(f"Spark job falló con código de salida: {result.returncode}")
            
    except Exception as e:
        context.log.error(f"❌ Error: {str(e)}")
        raise


# ============================================================================
# ASSETS - Diferentes ejemplos de Spark
# ============================================================================

@asset
def spark_pi_example(context: AssetExecutionContext) -> dict:
    """Ejemplo SparkPi - Calcula Pi usando Monte Carlo"""
    return run_spark_example(
        context=context,
        class_name="org.apache.spark.examples.SparkPi",
        job_name="SparkPi-Example",
        args=["1000"]  # Número de particiones
    )


@asset
def spark_word_count_example(context: AssetExecutionContext) -> dict:
    """Ejemplo WordCount - Cuenta palabras en un archivo"""
    # Nota: Este ejemplo requiere un archivo de entrada
    # Puedes modificar args para apuntar a tu archivo
    return run_spark_example(
        context=context,
        class_name="org.apache.spark.examples.JavaWordCount",
        job_name="WordCount-Example",
        args=[SPARK_EXAMPLES_JAR]  # Cuenta palabras del propio JAR
    )


@asset
def spark_pagerank_example(context: AssetExecutionContext) -> dict:
    """Ejemplo PageRank - Algoritmo de ranking de páginas"""
    return run_spark_example(
        context=context,
        class_name="org.apache.spark.examples.SparkPageRank",
        job_name="PageRank-Example",
        args=["100", "5"]  # páginas, iteraciones
    )


@asset
def spark_logistic_regression_example(context: AssetExecutionContext) -> dict:
    """Ejemplo de Regresión Logística - ML"""
    return run_spark_example(
        context=context,
        class_name="org.apache.spark.examples.ml.LogisticRegressionSummaryExample",
        job_name="LogisticRegression-Example",
        args=[]
    )


# ============================================================================
# JOBS - Definición de jobs para cada asset
# ============================================================================

spark_pi_job = define_asset_job(
    name="spark_pi_job",
    selection="spark_pi_example"
)

spark_wordcount_job = define_asset_job(
    name="spark_wordcount_job",
    selection="spark_word_count_example"
)

spark_pagerank_job = define_asset_job(
    name="spark_pagerank_job",
    selection="spark_pagerank_example"
)

spark_ml_job = define_asset_job(
    name="spark_ml_job",
    selection="spark_logistic_regression_example"
)


# ============================================================================
# SCHEDULES - Diferentes horarios de ejecución
# ============================================================================

# Ejecutar SparkPi cada día a las 2:00 AM
spark_pi_daily_schedule = ScheduleDefinition(
    name="spark_pi_daily",
    job=spark_pi_job,
    cron_schedule="0 2 * * *",
    description="Ejecuta SparkPi diariamente a las 2:00 AM"
)

# Ejecutar WordCount cada 6 horas
spark_wordcount_schedule = ScheduleDefinition(
    name="spark_wordcount_6h",
    job=spark_wordcount_job,
    cron_schedule="0 */6 * * *",
    description="Ejecuta WordCount cada 6 horas"
)

# Ejecutar PageRank cada lunes a las 3:00 AM
spark_pagerank_weekly_schedule = ScheduleDefinition(
    name="spark_pagerank_weekly",
    job=spark_pagerank_job,
    cron_schedule="0 3 * * 1",
    description="Ejecuta PageRank cada lunes a las 3:00 AM"
)

# Ejecutar ML job el primer día de cada mes
spark_ml_monthly_schedule = ScheduleDefinition(
    name="spark_ml_monthly",
    job=spark_ml_job,
    cron_schedule="0 4 1 * *",
    description="Ejecuta job de ML el primer día de cada mes a las 4:00 AM"
)


# ============================================================================
# DEFINITIONS - Repositorio de Dagster
# ============================================================================

defs = Definitions(
    assets=[
        spark_pi_example,
        spark_word_count_example,
        spark_pagerank_example,
        spark_logistic_regression_example,
    ],
    jobs=[
        spark_pi_job,
        spark_wordcount_job,
        spark_pagerank_job,
        spark_ml_job,
    ],
    schedules=[
        spark_pi_daily_schedule,
        spark_wordcount_schedule,
        spark_pagerank_weekly_schedule,
        spark_ml_monthly_schedule,
    ]
)
