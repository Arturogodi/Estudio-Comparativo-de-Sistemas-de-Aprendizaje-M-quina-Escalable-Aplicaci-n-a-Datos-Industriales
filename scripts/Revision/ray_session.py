import ray
from typing import Optional
from ray.util import ActorPool

def create_ray_session(
    num_cpus: Optional[int] = None,
    num_gpus: Optional[int] = None,
    memory: Optional[int] = None,
    object_store_memory: Optional[int] = None,
    dashboard_host: str = "0.0.0.0",
    dashboard_port: int = 8265
) -> ray:
    """
    Inicia una sesión Ray con configuraciones optimizadas para rendimiento y escalabilidad.
    
    Args:
        num_cpus (Optional[int]): Número de CPUs a usar. Si None, usa todos disponibles.
        num_gpus (Optional[int]): Número de GPUs a usar. Si None, usa todos disponibles.
        memory (Optional[int]): Memoria total en bytes.
        object_store_memory (Optional[int]): Memoria para el store de objetos en bytes.
        dashboard_host (str): Host para el dashboard de Ray.
        dashboard_port (int): Puerto para el dashboard de Ray.
        
    Returns:
        ray: Sesión Ray inicializada
    """
    # Inicializar Ray con configuraciones optimizadas
    ray.init(
        num_cpus=num_cpus,
        num_gpus=num_gpus,
        memory=memory,
        object_store_memory=object_store_memory,
        ignore_reinit_error=True,
        dashboard_host=dashboard_host,
        dashboard_port=dashboard_port,
        _system_config={
            # Timeout y reintentos
            "num_heartbeats_timeout": 10,
            "max_direct_call_object_size": 50 * 1024 * 1024,
            "max_object_size": 2 * 1024 * 1024 * 1024,
            "max_workers_per_node": 8,
            "max_direct_calls_per_task": 100,
            "max_task_retries": 3,
            "task_retry_delay_ms": 1000,
            "max_retries": 3,
            
            # Límites de tareas y actores
            "max_direct_task_returns": 100,
            "max_task_returns": 100,
            "max_actor_task_returns": 100,
            "max_task_submissions": 1000,
            "max_actor_task_submissions": 1000,
            
            # Fracciones máximas
            "max_pending_task_fraction": 0.1,
            "max_pending_actor_task_fraction": 0.1,
            "max_pending_actor_creation_tasks": 100,
            "max_pending_actor_creation_task_fraction": 0.1,
            
            # Configuraciones de memoria
            "object_store_memory_fraction": 0.6,
            "plasma_store_memory": 10000000000,  # 10GB
            "min_memory_per_worker": 500000000,  # 500MB
            "max_memory_fraction": 0.8,
            
            # Optimizaciones de rendimiento
            "num_workers_per_process": 2,
            "worker_lease_timeout_milliseconds": 10000,
            "worker_timeout_milliseconds": 30000,
            "worker_check_interval_ms": 1000,
            
            # Configuraciones de networking
            "raylet_socket_size": 1000000000,  # 1GB
            "raylet_socket_timeout_ms": 30000,
            "raylet_socket_send_buffer_size": 1000000000,  # 1GB
            "raylet_socket_recv_buffer_size": 1000000000,  # 1GB
            
            # Configuraciones de logging
            "max_log_file_size": 500000000,  # 500MB
            "max_num_log_files": 10,
            "log_to_driver": True,
            "log_rotation_size": 500000000,  # 500MB
            "log_rotation_max_files": 20,
            
            # Configuraciones de recursos
            "max_cpu_fraction_per_task": 0.8,
            "max_memory_fraction_per_task": 0.8,
            "max_gpu_fraction_per_task": 0.8,
            "max_resource_fraction_per_task": 0.8,
            
            # Configuraciones de tareas
            "max_task_attempts": 3,
            "max_task_retries": 3,
            "max_task_retries_per_node": 3,
            "max_task_retries_per_actor": 3,
            "max_task_retries_per_actor_per_node": 3
        }
    )
    
    return ray

# Ejemplo de uso
def main():
    """Ejemplo de cómo usar la función create_ray_session"""
    # Crear sesión Ray con configuración básica
    ray_session = create_ray_session(
        num_cpus=4,
        num_gpus=1,
        memory=8 * 1024 * 1024 * 1024,  # 8GB
        object_store_memory=4 * 1024 * 1024 * 1024  # 4GB
    )
    
    # Verificar que la sesión se ha creado correctamente
    print("Sesión Ray creada con éxito")
    print(f"Número de CPUs disponibles: {ray.available_resources().get('CPU', 0)}")
    print(f"Número de GPUs disponibles: {ray.available_resources().get('GPU', 0)}")

if __name__ == "__main__":
    main()
