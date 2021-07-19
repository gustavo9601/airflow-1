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

def pass_params_from_trigger(**context):
    # recibe los parametros como **kwargs dentro del indice dag_run en el key .conf
    values_pass_param = context['dag_run'].conf
    print(values_pass_param)

# Usando el contexto para crear el dag
with DAG('dag_trigger_pass_params',
         default_args=default_args,
         schedule_interval='@daily') as dag:
    # Iniciando el operator
    start = DummyOperator(task_id='start')

    """
    Pasar parametros desde el trigger solo funciona para los operadores que soportan templates literales
    """

    prueba_python = PythonOperator(task_id='prueba_python',
                                   python_callable=pass_params_from_trigger,
                                   provide_context=True)

    # Iniciando otro operator
    """
    dag_run.conf['variable_de_pruebas_bash']  // accede al JSON pasado por parametro al ejecutarlo manualmente
    """
    prueba_bash = BashOperator(task_id='prueba_bash',
                               bash_command="echo {{ dag_run.conf['variable_de_pruebas_bash'] or 123 }}")

# Define el orden de ejecucion de cada operator
start >> prueba_python >> prueba_bash
