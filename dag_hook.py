"""
Imports
"""
import pandas as pd
import logging

"""
Imports DAG
"""
from airflow.models import DAG

from airflow.utils import dates

"""
Imports operators
"""
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

"""
Imports hooks
"""
from airflow.hooks.postgres_hook import PostgresHook


# Parametros de configuracion inicial
# owner => dueño del dag
# start_date => fecha en que iniciara la ejecucion
# dates.days_ago(1) // calcula el dia anterior
default_args = {
    'owner': 'gustavo_marquez',
    'start_date': dates.days_ago(1)
}

def get_pandas():
    # postgres_test // pasando el nombre de la conexion configurada
    connection = PostgresHook('postgres_test')
    df = connection.get_pandas_df('SELECT * FROM table')
    logging.info('Datos obtenidos del query')
    df.to_csv('./export_data.csv', index=False)
    logging.info('Datos exportados al csv ')
    

# Usando el contexto para crear el dag
with DAG('dag_test_hook',
         default_args=default_args,
         schedule_interval='@daily') as dag:

    # Iniciando el operator
    start_dummy = DummyOperator(task_id='start_dummy')
    # Iniciando otro operator, que llamara un callback
    get_pandas_operator = PythonOperator(task_id='get_pandas_operator',
                                         pyton_callable=get_pandas)

    end_dummy = DummyOperator(task_id='end_dummy')

# Definiendo flujo
start_dummy >> get_pandas_operator >> end_dummy