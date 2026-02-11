"""
Dagster job que ejecuta un ejemplo de Spark usando Spark Operator en Kubernetes
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
from typing import Dict


# Configuración del Spark Operator en K8s
SPARK_NAMESPACE = os.environ.get("SPARK_NAMESPACE", "data-systems")
SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "apache/spark:3.5.1")
SPARK_SERVICE_ACCOUNT = os.environ.get("SPARK_SERVICE_ACCOUNT", "spark")
SPARK_EXAMPLES_JAR = "local:///opt/spark/examples/jars/spark-examples_2.12-3.5.1.jar"


def create_spark_application_manifest(
    name: str,
    main_class: str,
    args: list = None,
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


def submit_spark_application(context: AssetExecutionContext, manifest: Dict, app_name: str) -> Dict:
    """
    Envía una SparkApplication al cluster usando Kubernetes Python Client
    
    Args:
        context: Contexto de Dagster
        manifest: Manifest de SparkApplication
        app_name: Nombre de la aplicación
    
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
        context.log.info(f"⏳ Esperando a que el Spark Operator procese la aplicación...")
        time.sleep(15)  # Dar tiempo al operator para inicializar
        
        context.log.info(f"⏳ Monitoreando ejecución de {app_name}...")
        max_wait = 300  # 5 minutos
        start_time = time.time()
        unknown_count = 0
        
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
                
                state = spark_app.get("status", {}).get("applicationState", {}).get("state", "")
                elapsed = int(time.time() - start_time)
                
                # Si no hay estado todavía, es normal al inicio
                if not state or state == "":
                    context.log.info(f"📊 Estado: pendiente (transcurridos {elapsed}s)")
                    time.sleep(10)
                    continue
                    
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
                elif state == "UNKNOWN":
                    unknown_count += 1
                    if unknown_count > 10:  # Después de 10 intentos con UNKNOWN, fallar
                        context.log.error(f"❌ Estado UNKNOWN persistente después de {unknown_count} intentos")
                        raise Exception(f"SparkApplication en estado UNKNOWN por mucho tiempo")
                    context.log.warning(f"⚠️  Estado: UNKNOWN (intento {unknown_count}/10)")
                    time.sleep(10)
                    continue
                elif state in ["FAILED", "SUBMISSION_FAILED", "INVALIDATING"]:
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


@asset
def execute_spark_pi_job(context: AssetExecutionContext) -> dict:
    """
    Ejecuta el job de ejemplo SparkPi usando Spark Operator
    
    El ejemplo SparkPi calcula el valor de Pi usando el método Monte Carlo.
    
    Returns:
        dict: Resultado de la ejecución
    """
    context.log.info("="*80)
    context.log.info("🚀 Iniciando ejecución de Spark Job - SparkPi")
    context.log.info(f"☸️  Usando Spark Operator en namespace: {SPARK_NAMESPACE}")
    context.log.info(f"🐳 Imagen: {SPARK_IMAGE}")
    context.log.info("="*80)
    
    app_name = f"spark-pi-dagster-{int(time.time())}"
    
    # Crear manifest de SparkApplication
    manifest = create_spark_application_manifest(
        name=app_name,
        main_class="org.apache.spark.examples.SparkPi",
        args=["100"],  # Número de particiones
        driver_memory="512m",
        executor_memory="768m",
        executor_instances=1
    )
    
    context.log.info("📄 Manifest de SparkApplication:")
    context.log.info("-"*80)
    context.log.info(f"  Nombre: {app_name}")
    context.log.info(f"  Clase: {manifest['spec']['mainClass']}")
    context.log.info(f"  Argumentos: {manifest['spec']['arguments']}")
    context.log.info(f"  Driver: {manifest['spec']['driver']['memory']}, {manifest['spec']['driver']['cores']} cores")
    context.log.info(f"  Executors: {manifest['spec']['executor']['instances']} x {manifest['spec']['executor']['memory']}")
    context.log.info("-"*80)
    
    # Enviar SparkApplication
    start_time = datetime.now()
    context.log.info(f"⏰ Hora de inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    
    result = submit_spark_application(context, manifest, app_name)
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    context.log.info("")
    context.log.info("="*80)
    context.log.info("📊 RESULTADO DE LA EJECUCIÓN")
    context.log.info("="*80)
    context.log.info(f"⏰ Hora de finalización: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
    context.log.info(f"⏱️  Duración: {duration:.2f} segundos")
    context.log.info(f"✅ SparkApplication: {app_name}")
    context.log.info("="*80)
    
    result["duration_seconds"] = duration
    result["start_time"] = start_time.isoformat()
    result["end_time"] = end_time.isoformat()
    
    return result


# Definir el job que ejecuta el asset
spark_pi_job = define_asset_job(
    name="spark_pi_job",
    selection="execute_spark_pi_job",
    description="Job que ejecuta el ejemplo SparkPi usando Spark Operator"
)


# Definir el schedule con cron
# Este schedule ejecutará el job cada día a las 2:00 AM
spark_pi_schedule = ScheduleDefinition(
    name="spark_pi_daily_schedule",
    job=spark_pi_job,
    cron_schedule="0 2 * * *",  # Cada día a las 2:00 AM
    description="Ejecuta el job SparkPi diariamente a las 2:00 AM"
)

# Otros ejemplos de cron schedules:
# "*/15 * * * *"  - Cada 15 minutos
# "0 */2 * * *"   - Cada 2 horas
# "0 9 * * 1-5"   - Cada día laboral a las 9:00 AM
# "0 0 * * 0"     - Cada domingo a medianoche
# "0 12 1 * *"    - El primer día de cada mes al mediodía


# Definición del repositorio de Dagster
defs = Definitions(
    assets=[execute_spark_pi_job],
    jobs=[spark_pi_job],
    schedules=[spark_pi_schedule]
)
