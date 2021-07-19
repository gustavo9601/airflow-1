"""
Imports DAG
"""
from airflow.models import DAG

"""
Imports operators
"""
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import BranchPythonOperator

from datetime import datetime
import ast

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


def branch_get_data(**context):
    task_instance = context['task_instance']
    # obteniendo lo enviado por el otro operador via xcom
    api_response = task_instance.xcom_pull(task_ids='api_call')

    # ast.literal_eval // evalua el string y lo trasnforma al objeto propio enviado
    response_eval_api = ast.literal_eval(api_response)
    number = response_eval_api[0]['random']  # obtenemos el valor random

    # logica de la rama, a ejecutar si ocurre un suceso u otro
    # debe retornar el task id de la tarea a ejecutar
    if number > 5:
        return 'greater_than_5'
    else:
        return 'less_than_5'


# Usando el contexto para crear el dag
with DAG('dag_test_multiple_branch_gm',
         default_args=default_args,
         schedule_interval='@daily') as dag:
    # Iniciando el operator
    start = DummyOperator(task_id='start')

    # http_conn_id='random_number' // id de conexion creada desde el dashboard
    # data // diccionario con los parametros que recibe el endpoint
    # api => https://csrng.net/csrng/csrng.php?min=0&max=151&status=error
    api_call = SimpleHttpOperator(
        task_id='api_call',
        http_conn_id='random_number',
        endpoint='csrng/csrng.php',
        method='GET',
        data={'min': '0', 'max': '10'},
        xcom_push=True
    )

    # Creando otra rama de procesos
    # python_callable // especificando el callback que recibira los datos enviados por xcom
    branch_api = BranchPythonOperator(
        task_id='branch_api',
        python_callable=branch_get_data,
        provide_context=True
    )

    # operadores que se invocaran desde otra rama
    greater_than_5 = DummyOperator(task_id='greater_than_5')
    less_than_5 = DummyOperator(task_id='less_than_5')

# Define el orden de ejecucion de cada operator
# >> [greater_than_5, less_than_5] // especifica que puede ejecutarce cualquiera dependiendo de lo retornado por branch_api
start >> api_call >> branch_api >> [greater_than_5, less_than_5]
