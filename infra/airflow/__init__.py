"""Apache Airflow helpers (optional dependency)."""

from .compat import airflow_available, require_airflow
from .defaults import default_dag_args

__all__ = [
    "airflow_available",
    "default_dag_args",
    "require_airflow",
]
