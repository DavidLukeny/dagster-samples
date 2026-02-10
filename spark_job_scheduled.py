"""
Dagster job que ejecuta un ejemplo de Spark con schedule (cron)
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


# Configuración de Spark para K3s
# El spark-submit debe existir en la máquina local para enviar jobs al cluster K3s
SPARK_SUBMIT = os.environ.get("SPARK_SUBMIT", "spark-submit")

# Configuración del cluster K3s
# K3s por defecto usa el puerto 6443
K8S_MASTER = os.environ.get("SPARK_K8S_MASTER", "k8s://https://127.0.0.1:6443")
SPARK_NAMESPACE = os.environ.get("SPARK_NAMESPACE", "default")
SPARK_SERVICE_ACCOUNT = os.environ.get("SPARK_SERVICE_ACCOUNT", "spark")

# Imagen Docker de Spark para K3s
# Puede usar imágenes del registry local de K3s o public registries
SPARK_IMAGE = os.environ.get("SPARK_IMAGE", "apache/spark:latest")

# Ejemplo por defecto de Spark - usar URL local o remoto del JAR
SPARK_EXAMPLES_JAR = os.environ.get("SPARK_EXAMPLES_JAR", "local:///opt/spark/examples/jars/spark-examples.jar")


@asset
def execute_spark_pi_job(context: AssetExecutionContext) -> dict:
    """
    Ejecuta el job de ejemplo SparkPi de Spark
    
    El ejemplo SparkPi calcula el valor de Pi usando el método Monte Carlo.
    
    Returns:
        dict: Resultado de la ejecución con stdout, stderr y código de salida
    """
    context.log.info("="*80)
    context.log.info("🚀 Iniciando ejecución de Spark Job - SparkPi")
    context.log.info("="*80)
    
    # Comando spark-submit para Kubernetes
    # Ejecuta el ejemplo SparkPi con 100 particiones en el cluster K8s
    command = [
        SPARK_SUBMIT,
        "--master", K8S_MASTER,
        "--deploy-mode", "cluster",
        "--name", "SparkPi-Dagster-Job",
        "--class", "org.apache.spark.examples.SparkPi",
        "--conf", f"spark.kubernetes.namespace={SPARK_NAMESPACE}",
        "--conf", f"spark.kubernetes.container.image={SPARK_IMAGE}",
        "--conf", f"spark.kubernetes.authenticate.driver.serviceAccountName={SPARK_SERVICE_ACCOUNT}",
        "--conf", "spark.executor.instances=2",
        "--conf", "spark.executor.memory=1g",
        "--conf", "spark.executor.cores=1",
        "--conf", "spark.driver.memory=1g",
        SPARK_EXAMPLES_JAR,
        "100"  # Número de particiones para el cálculo
    ]
    
    context.log.info(f"📝 Comando a ejecutar:")
    context.log.info(f"   {' '.join(command)}")
    context.log.info("")
    
    try:
        context.log.info(f"📦 Usando JAR: {SPARK_EXAMPLES_JAR}")
        context.log.info(f"☸️  Cluster K3s: {K8S_MASTER}")
        context.log.info(f"📦 Namespace: {SPARK_NAMESPACE}")
        context.log.info(f"🐳 Imagen: {SPARK_IMAGE}")
        
        # Ejecutar el comando
        start_time = datetime.now()
        context.log.info(f"⏰ Hora de inicio: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=300  # timeout de 5 minutos
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
        context.log.info("")
        
        # Mostrar stdout
        if result.stdout:
            context.log.info("📤 STDOUT:")
            context.log.info("-"*80)
            for line in result.stdout.split('\n'):
                context.log.info(line)
            context.log.info("-"*80)
        
        # Mostrar stderr (Spark escribe logs en stderr)
        if result.stderr:
            context.log.info("")
            context.log.info("📋 LOGS DE SPARK:")
            context.log.info("-"*80)
            # Filtrar líneas importantes
            for line in result.stderr.split('\n'):
                if 'Pi is roughly' in line or 'ERROR' in line or 'INFO SparkContext' in line:
                    context.log.info(line)
            context.log.info("-"*80)
        
        # Verificar si fue exitoso
        if result.returncode == 0:
            context.log.info("")
            context.log.info("✅ Job de Spark ejecutado exitosamente!")
            
            # Intentar extraer el resultado de Pi
            pi_value = None
            for line in result.stdout.split('\n'):
                if 'Pi is roughly' in line:
                    context.log.info(f"🎯 {line}")
                    # Extraer el valor
                    pi_value = line.split('roughly')[1].strip()
                    break
            
            return {
                "status": "success",
                "exit_code": result.returncode,
                "duration_seconds": duration,
                "pi_value": pi_value,
                "start_time": start_time.isoformat(),
                "end_time": end_time.isoformat(),
                "stdout": result.stdout,
                "stderr": result.stderr
            }
        else:
            context.log.error("")
            context.log.error("❌ Error en la ejecución del job de Spark")
            raise Exception(f"Spark job falló con código de salida: {result.returncode}")
            
    except subprocess.TimeoutExpired:
        context.log.error("⏰ El job de Spark excedió el tiempo límite de ejecución")
        raise
    except FileNotFoundError as e:
        context.log.error(f"❌ Error: {str(e)}")
        context.log.error(f"💡 Verifica que SPARK_HOME esté configurado correctamente: {SPARK_HOME}")
        raise
    except Exception as e:
        context.log.error(f"❌ Error inesperado: {str(e)}")
        raise


# Definir el job que ejecuta el asset
spark_pi_job = define_asset_job(
    name="spark_pi_job",
    selection="execute_spark_pi_job",
    description="Job que ejecuta el ejemplo SparkPi de Spark"
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
