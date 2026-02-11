"""
Dagster jobs para ejecutar diferentes ejemplos de Spark usando Spark Operator
"""
from dagster import (
    asset,
    AssetExecutionContext,
    define_asset_job,
    ScheduleDefinition,
    Definitions,
)
from kubernetes import client, config
import os
import time
from datetime import datetime
from typing import Dict, List, Optional


# Configuración del Spark Operator en K8s
SPARK_NAMESPACE = os.environ.get("SPARK_NAMESPACE", "data-systems")
SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "apache/spark:3.5.1")
SPARK_SERVICE_ACCOUNT = os.environ.get("SPARK_SERVICE_ACCOUNT", "spark")
SPARK_EXAMPLES_JAR = "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar"


def create_spark_application_manifest(
    name: str,
    main_class: str,
    args: List[str] = None,
    driver_memory: str = "512m",
    executor_memory: str = "768m",
    executor_instances: int = 1,
) -> Dict:
    """
    Crea un manifest de SparkApplication para el Spark Operator
    
    Args:
        name: Nombre de la aplicación
        main_class: Clase principal de Spark a ejecutar
        args: Argumentos para la aplicación
        driver_memory: Memoria del driver
        executor_memory: Memoria de los executors
        executor_instances: Número de executors
    
    Returns:
        Dict: Manifest de SparkApplication
    """
    manifest = {
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": name,
            "namespace": SPARK_NAMESPACE
        },
        "spec": {
            "type": "Scala",
            "mode": "cluster",
            "image": SPARK_IMAGE,
            "imagePullPolicy": "IfNotPresent",
            "mainClass": main_class,
            "mainApplicationFile": SPARK_EXAMPLES_JAR,
            "sparkVersion": "3.5.1",
            "arguments": args or [],
            "sparkConf": {
                "spark.ui.port": "4040"
            },
            "restartPolicy": {
                "type": "Never"
            },
            "driver": {
                "cores": 1,
                "memory": driver_memory,
                "serviceAccount": SPARK_SERVICE_ACCOUNT,
                "labels": {"app": "spark"},
                "nodeSelector": {
                    "kubernetes.io/arch": "arm64"
                }
            },
            "executor": {
                "instances": executor_instances,
                "cores": 1,
                "memory": executor_memory,
                "nodeSelector": {
                    "kubernetes.io/arch": "arm64"
                }
            }
        }
    }
    
    return manifest


def submit_spark_application(
    context: AssetExecutionContext,
    manifest: Dict,
    app_name: str,
    max_wait: int = 300
) -> Dict:
    """
    Envía una SparkApplication al cluster usando Kubernetes Python Client
    
    Args:
        context: Contexto de Dagster
        manifest: Manifest de SparkApplication
        app_name: Nombre de la aplicación
        max_wait: Tiempo máximo de espera en segundos
    
    Returns:
        Dict con información de la ejecución
    """
    try:
        # Cargar configuración in-cluster (usa ServiceAccount automáticamente)
        config.load_incluster_config()
        
        # Crear cliente de CustomObjects para CRDs
        api = client.CustomObjectsApi()
        
        # Crear el recurso SparkApplication
        context.log.info(f"📝 Creando SparkApplication: {app_name}")
        api.create_namespaced_custom_object(
            group="sparkoperator.k8s.io",
            version="v1beta2",
            namespace=SPARK_NAMESPACE,
            plural="sparkapplications",
            body=manifest
        )
        
        context.log.info(f"✅ SparkApplication creada exitosamente")
        
        # Esperar y monitorear el estado
        context.log.info(f"⏳ Monitoreando ejecución de {app_name}...")
        
        start_time = time.time()
        
        while (time.time() - start_time) < max_wait:
            try:
                # Obtener estado de la SparkApplication
                spark_app = api.get_namespaced_custom_object(
                    group="sparkoperator.k8s.io",
                    version="v1beta2",
                    namespace=SPARK_NAMESPACE,
                    plural="sparkapplications",
                    name=app_name
                )
                
                state = spark_app.get("status", {}).get("applicationState", {}).get("state", "UNKNOWN")
                elapsed = int(time.time() - start_time)
                context.log.info(f"📊 Estado: {state} (transcurridos {elapsed}s)")
                
                if state == "COMPLETED":
                    context.log.info(f"✅ SparkApplication {app_name} completada exitosamente!")
                    return {
                        "status": "success",
                        "app_name": app_name,
                        "state": state,
                        "namespace": SPARK_NAMESPACE,
                        "elapsed_seconds": elapsed
                    }
                elif state in ["FAILED", "SUBMISSION_FAILED", "INVALIDATING", "UNKNOWN"]:
                    context.log.error(f"❌ SparkApplication {app_name} falló con estado: {state}")
                    
                    # Intentar obtener logs del driver
                    context.log.info("📋 Buscando logs del driver...")
                    try:
                        core_api = client.CoreV1Api()
                        pods = core_api.list_namespaced_pod(
                            namespace=SPARK_NAMESPACE,
                            label_selector=f"sparkoperator.k8s.io/app-name={app_name},spark-role=driver"
                        )
                        
                        if pods.items:
                            driver_pod = pods.items[0]
                            logs = core_api.read_namespaced_pod_log(
                                name=driver_pod.metadata.name,
                                namespace=SPARK_NAMESPACE,
                                tail_lines=50
                            )
                            context.log.error("📋 Últimos logs del driver:")
                            for line in logs.split('\n')[-30:]:
                                if line.strip():
                                    context.log.error(line)
                    except Exception as log_error:
                        context.log.warning(f"No se pudieron obtener logs: {log_error}")
                    
                    raise Exception(f"SparkApplication falló con estado: {state}")
                
                # Estados en progreso
                elif state in ["SUBMITTED", "RUNNING", "PENDING_RERUN", "SUCCEEDING"]:
                    time.sleep(10)
                    continue
                    
            except client.exceptions.ApiException as e:
                if e.status == 404:
                    context.log.warning(f"SparkApplication {app_name} no encontrada aún, esperando...")
                    time.sleep(5)
                    continue
                raise
            
            time.sleep(5)
        
        # Timeout
        context.log.error(f"⏰ Timeout esperando completación de {app_name}")
        raise Exception(f"SparkApplication no completó en {max_wait} segundos")
        
    except Exception as e:
        context.log.error(f"❌ Error: {str(e)}")
        raise
    finally:
        # Limpiar: eliminar la SparkApplication
        context.log.info(f"🧹 Limpiando SparkApplication {app_name}...")
        try:
            api.delete_namespaced_custom_object(
                group="sparkoperator.k8s.io",
                version="v1beta2",
                namespace=SPARK_NAMESPACE,
                plural="sparkapplications",
                name=app_name
            )
        except Exception as cleanup_error:
            context.log.warning(f"Error durante limpieza: {cleanup_error}")


def run_spark_example(
    context: AssetExecutionContext,
    class_name: str,
    job_name: str,
    args: List[str] = None,
    driver_memory: str = "512m",
    executor_memory: str = "768m",
    executor_instances: int = 1,
    max_wait: int = 300
) -> Dict:
    """
    Función auxiliar para ejecutar ejemplos de Spark usando Spark Operator
    
    Args:
        context: Contexto de ejecución de Dagster
        class_name: Nombre completo de la clase de Spark a ejecutar
        job_name: Nombre descriptivo del job
        args: Argumentos para pasar al job de Spark
        driver_memory: Memoria del driver
        executor_memory: Memoria de los executors
        executor_instances: Número de executors
        max_wait: Tiempo máximo de espera en segundos
    """
    context.log.info("="*80)
    context.log.info(f"🚀 Iniciando ejecución de Spark Job - {job_name}")
    context.log.info(f"☸️  Usando Spark Operator en namespace: {SPARK_NAMESPACE}")
    context.log.info(f"🐳 Imagen: {SPARK_IMAGE}")
    context.log.info(f"📦 Clase: {class_name}")
    context.log.info("="*80)
    
    # Crear nombre único para la aplicación
    timestamp = int(time.time())
    safe_job_name = job_name.lower().replace(" ", "-").replace("_", "-")
    app_name = f"{safe_job_name}-{timestamp}"
    
    # Crear manifest de SparkApplication:")
    context.log.info("-"*80)
    context.log.info(f"  Nombre: {app_name}")
    context.log.info(f"  Clase: {manifest['spec']['mainClass']}")
    context.log.info(f"  Argumentos: {manifest['spec']['arguments']}")
    context.log.info(f"  Driver: {manifest['spec']['driver']['memory']}, {manifest['spec']['driver']['cores']} cores")
    context.log.info(f"  Executors: {manifest['spec']['executor']['instances']} x {manifest['spec']['executor']['memory']}",
        args=args or [],
        driver_memory=driver_memory,
        executor_memory=executor_memory,
        executor_instances=executor_instances
    )
    
    context.log.info("📄 SparkApplication manifest (primeras líneas):")
    context.log.info("-"*80)
    for line in manifest.split('\n')[:20]:
        context.log.info(line)
    context.log.info("-"*80)
    
    # Enviar SparkApplication
    start_time = datetime.now()
    context.log.info(f"⏰ Hora de inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    result = submit_spark_application(context, manifest, app_name, max_wait)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    context.log.info("")
    context.log.info("="*80)
    context.log.info("📊 RESULTADO DE LA EJECUCIÓN")
    context.log.info("="*80)
    context.log.info(f"⏰ Hora de finalización: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    context.log.info(f"⏱️  Duración total: {duration:.2f} segundos")
    context.log.info(f"✅ Job: {job_name}")
    context.log.info("="*80)
    
    result["job_name"] = job_name
    result["duration_seconds"] = duration
    result["start_time"] = start_time.isoformat()
    result["end_time"] = end_time.isoformat()
    
    return result


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
        args=["1000"],  # Número de particiones
        executor_instances=1
    )


@asset
def spark_pagerank_example(context: AssetExecutionContext) -> dict:
    """Ejemplo PageRank - Algoritmo de ranking de páginas"""
    return run_spark_example(
        context=context,
        class_name="org.apache.spark.examples.SparkPageRank",
        job_name="PageRank-Example",
        args=["/opt/spark/data/", "5"],  # directorio de datos, iteraciones
        executor_instances=2,
        max_wait=600  # 10 minutos
    )


@asset
def spark_logistic_regression_example(context: AssetExecutionContext) -> dict:
    """Ejemplo de Regresión Logística - ML"""
    return run_spark_example(
        context=context,
        class_name="org.apache.spark.examples.ml.LogisticRegressionExample",
        job_name="LogisticRegression-Example",
        args=[],
        driver_memory="1g",
        executor_memory="1g",
        executor_instances=2
    )


# ============================================================================
# JOBS - Definición de jobs para cada asset
# ============================================================================

spark_pi_job = define_asset_job(
    name="spark_pi_job",
    selection="spark_pi_example",
    description="Ejecuta el ejemplo SparkPi usando Spark Operator"
)

spark_pagerank_job = define_asset_job(
    name="spark_pagerank_job",
    selection="spark_pagerank_example",
    description="Ejecuta el ejemplo PageRank usando Spark Operator"
)

spark_ml_job = define_asset_job(
    name="spark_ml_job",
    selection="spark_logistic_regression_example",
    description="Ejecuta el ejemplo de ML usando Spark Operator"
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
        spark_pagerank_example,
        spark_logistic_regression_example,
    ],
    jobs=[
        spark_pi_job,
        spark_pagerank_job,
        spark_ml_job,
    ],
    schedules=[
        spark_pi_daily_schedule,
        spark_pagerank_weekly_schedule,
        spark_ml_monthly_schedule,
    ]
)
