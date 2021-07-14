"""
Imports DAG
"""
from airflow.models import DAG

"""
Imports operators
"""
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
    grettings = ''
    for idx, word in enumerate(['hello', 'world']):
        print(f"[{idx}] : [{word}]")
        grettings += f"{word} "
    # Debe retornar algo, para que sea recibido por el Xcom
    return grettings


def prueba_pull(**context):
    # accedemos a la propiedad
    task_instance = context['task_instance']
    # para obtener el retorno de un proceso del workflow especificamos su task_id
    value_pull = task_instance.xcom_pull(task_ids='prueba_python')

    print(f'Data obtenida desde el callback, {value_pull}')


# Usando el contexto para crear el dag
with DAG('dag_test_comunicate_workflows',
         default_args=default_args,
         schedule_interval='@daily') as dag:
    # Inciando otro operator
    # pasando una funcion callback a ejecutar
    # do_xcom_push // notifica a workflow retornara un valor desde el callback
    prueba_python = PythonOperator(task_id='prueba_python',
                                   python_callable=hello_world_loop,
                                   do_xcom_push=True)

    #   provide_context=True // especifica que el callback recibira parametros emitidos por otros workflows
    prueba_pull_workflow = PythonOperator(task_id='prueba_pull_workflow',
                                          python_callable=prueba_pull,
                                          provide_context=True,
                                          do_xcom_push=True)

    # Iniciando otro operator que ejecuta via bash un script
    # bash_command // recibe la ruta y el comando a ejecutar
    prueba_bash = BashOperator(task_id='prueba_bash',
                               do_xcom_push=True,
                               bash_command='python C/Users/gustavomp/Pictures/bd/docker_files/test.py')

# Define el orden de ejecucion de cada operator
prueba_python >> prueba_pull_workflow >> prueba_bash

# Otra forma
# [prueba_python, prueba_pull_workflow, prueba_bash]

# Creando otra rama de proceso al tiempo
# prueba_python >> prueba_pull_workflow
# prueba_python >> prueba_bash
# // se toma los ultimos task_id que determinan el fin del proceso
#[prueba_pull_workflow, prueba_bash] >> fin_proceso_task_id

