"""
Imports DAG
"""
from airflow.models import DAG

"""
Imports operators
"""
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime

# Parametros de configuracion inicial
# owner => dueño del dag
# start_date => fecha en que iniciara la ejecucion
default_args = {
    'owner': 'gustavo_marquez',
    'start_date': datetime(2021, 7, 10, 10, 0, 0)
}


def hello_world_loop():
    for idx, word in enumerate(['hello', 'world']):
        print(f"[{idx}] : [{word}]")


# Usando el contexto para crear el dag
with DAG('dag_test_gm',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    # Iniciando el operator
    start = DummyOperator(task_id='start')
    # Inciando otro operator
    # pasando una funcion callback a ejecutar
    prueba_python = PythonOperator(task_id='prueba_python',
                                   python_callable=hello_world_loop)
    # Iniciando otro operator
    prueba_bash = BashOperator(task_id='prueba_bash',
                               bash_command='echo prueba_bash')


# Define el orden de ejecucion de cada operator
start >> prueba_python >> prueba_bash
