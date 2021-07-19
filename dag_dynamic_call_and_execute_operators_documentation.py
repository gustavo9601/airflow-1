"""
Imports DAG
"""
from airflow.models import DAG

"""
Imports operators
"""
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator

from datetime import datetime

# Parametros de configuracion inicial
# owner => dueño del dag
# start_date => fecha en que iniciara la ejecucion
# pool => nombre del pool definido en el dashboard, para paralelismo
default_args = {
    'owner': 'gustavo_marquez',
    'start_date': datetime(2021, 7, 10, 10, 0, 0),
    'pool': 'high_priority'
}

# Usando el contexto para crear el dag
with DAG('dag_test_dynamic_call_operators',
         default_args=default_args,
         schedule_interval='@daily') as dag:
    operators_list = []

    start = DummyOperator(task_id='start')

    operators_list.append(start)

    for country in ['colombia', 'puerto_rico', 'venezuela', 'usa']:
        # Creando el operador dinamicamente
        country_operator = BashOperator(task_id=f'country_{country}',
                                        bash_command=f'echo {country}')
        operators_list.append(country_operator)

    end = DummyOperator(task_id='end')

    operators_list.append(end)


    # añadiendo los comentarios al inicio del script como documentacion para visualizar en el dashboard
    """
    dag.dog_md = __doc__ 
    dag.dog_md = 'documentation string'   
    """
    dag.dog_md = __doc__

    # añadiendo documentacion a un operadtor puntual
    """
    start.doc_md = 'documentation string'
    """


# creando linealmente pero dinamicamente el llamado de los diferentes operadores
for i in range(len(operators_list) - 1):
    # llamado dinamico de la lista de operadores
    operators_list[i] >> operators_list[i + 1]

