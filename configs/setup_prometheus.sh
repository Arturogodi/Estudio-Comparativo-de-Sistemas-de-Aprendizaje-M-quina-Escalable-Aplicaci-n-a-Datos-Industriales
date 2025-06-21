#!/bin/bash

# Asegura que la variable esté definida
: "${JMX_PORT:?Debe definir JMX_PORT}"

echo "🛠️ Configurando Prometheus JMX Agent en el puerto $JMX_PORT"

# Solo configurar si no está ya presente
export SPARK_DAEMON_JAVA_OPTS="-javaagent:/opt/spark/jars/jmx_prometheus_javaagent-0.20.0.jar=${JMX_PORT}:/configs/prometheus-jmx-config.yaml"

echo "✅ Prometheus setup completed"
