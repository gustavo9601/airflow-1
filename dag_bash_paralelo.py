import json

from airflow.models import DAG

from airflow.operators.bash_operator import BashOperator

from datetime import datetime

default_args = {
    'name_dag': 'dag_bash_paralelo',
    'schedule_interval': '@daily',
    'start_date': datetime(2021, 10, 1),
    'catchup': False
}

# IMPORTANTE
# catchup // control que permite ejecutar todos los dags pasados si se llega a detener el dag o algun proceso

with DAG(
        default_args['name_dag'],
        schedule_interval=default_args['schedule_interval'],
        start_date=default_args['start_date'],
        catchup=default_args['catchup'],
) as dag:
    # Tareas a realizar

    tarea_1 = BashOperator(
        task_id='tarea_1',
        bash_command='sleep 3'
    )
    tarea_2 = BashOperator(
        task_id='tarea_2',
        bash_command='sleep 4'
    )
    tarea_3 = BashOperator(
        task_id='tarea_3',
        bash_command='sleep 5'
    )
    tarea_4 = BashOperator(
        task_id='tarea_4',
        bash_command='echo "finalizado"'
    )

# Orden de ejecucion
tarea_1 >> [tarea_2, tarea_3] >> tarea_4
