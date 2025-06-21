# TFG-Ray-vs-Spark

## Configuración Local

### Entorno Virtual y Dependencias
```powershell
# Crear y activar entorno virtual
python -m venv venv
.\venv\Scripts\activate

# Instalar dependencias
pip install -r requirements.txt
```

## Configuración de Clusters

### Dask
```powershell
# Levantar cluster con 2 workers (Configuración C1)
docker-compose -f docker-compose_C1_Dask.yml up -d --build
```

### Ray
```powershell
# Levantar cluster con 2 workers (Configuración C1)
docker-compose -f docker-compose_C1_Ray.yml up -d --build

```

### Spark
```powershell
# Levantar cluster con 2 workers (Configuración C1)
docker-compose -f docker-compose_C1_Spark.yml up -d --build
```

## Requisitos del Sistema

### Versión de Python
- Python 3.9.13 (estandarizado para todas las configuraciones)

### Dependencias Principales
- Ray 2.44.1
- Spark 3.5.5
- Dask 2024.8.0
- Pandas 2.2.3
- NumPy 2.0.2

### Configuraciones de Ejecución
| Configuración | # Workers | Cores/Worker | RAM/Worker | Total Cores | Total RAM | Driver Core | Driver RAM |
|---------------|-----------|--------------|------------|-------------|-----------|-----------|-----------|
| **C1** | 5 | 2 | 10 GB | 10 | 50 GB | 2 | 4 GB |


## Estructura del Proyecto
```
├── data/           # Datos de entrada/salida
├── scripts/        # Scripts de procesamiento
│   ├── Ray/        # Scripts Ray
│   ├── Spark/      # Scripts Spark
│   └── Dask/       # Scripts Dask
│   └── root/       # Scripts generales uso fuera de contenedores
├── configs/        # Configuraciones
├── docker-compose*.yml  # Configuraciones Docker
└── requirements.txt     # Dependencias Python


## Notas Técnicas

### Requisitos para Ray
- **Imagen usada:** rayproject/ray:2.44.1
- **Python:** 3.9.21 en contenedor, 3.9.13 en cliente
- **Instalación Ray Client:** `pip install "ray[client]==2.44.1"`

### Requisitos para Spark
- **Python:** 3.9.13 (versión estandarizada)
- **Spark:** 3.5.5
- **PySpark:** 3.5.5

### Mantenimiento de Contenedores Ray
- **Comando de arranque:**
```yaml
command: >
  bash -c "ray start --head --port=6379 --dashboard-host 0.0.0.0 --dashboard-port=8265 --ray-client-server-port=10001 --disable-usage-stats && tail -f /dev/null"
```

### Monitoreo
#### Usar en power shell desde root o desde scripts/
- **scripts\monitor_container_stats.py**
- **scripts\monitor_container_stats_ray.py**
- **scripts\monitor_container_stats_dask.py**

Opcional:
- **Prometheus:** v2.47.2
- **Grafana:** v9.4.3
- **cAdvisor:** v0.46.0
- **Node Exporter:** v1.6.1

## Estructura de Directorios
```
├── data/           # Datos de entrada/salida
├── scripts/        # Scripts de procesamiento
├── helpers/        # Funciones auxiliares
├── logs/           # Logs de ejecución
├── configs/        # Configuraciones
├── grafana/        # Configuraciones Grafana
├── docker-compose*.yml  # Configuraciones Docker
└── requirements.txt     # Dependencias Python
```