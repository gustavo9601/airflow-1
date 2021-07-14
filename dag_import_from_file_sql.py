"""
Imports DAG
"""
from airflow.models import DAG
from airflow.utils import dates

"""
Import operators
"""
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Parametros de configuracion inicial
# owner => dueño del dag
# start_date => fecha en que iniciara la ejecucion
# dates.days_ago(1) // calcula el dia anterior
default_args = {
    'owner': 'gustavo_marquez',
    'start_date': dates.days_ago(1)
}


# Usando el contexto para crear el dag
# template_searchpath // especifica el directorio donde buscara el scrip a ejecutar
with DAG('dag_test_import_sql',
         default_args=default_args,
         template_searchpath=['/home/name_user/airflow/folder/scripts'],
         schedule_interval='@daily') as dag:

    """
       sql='name_file.sql', // archivo que buscara en la ruta anteriormente defenida a ejecutar
        postgres_conn_id='name_connection_configurated', // nombre de la conexion configurada en el dashboard
    """
    generate_train_and_test_Tables = PostgresOperator(
        task_id='generate_train_and_test_Tables',
        sql='name_file.sql',
        postgres_conn_id='name_connection_configurated',
        autocomit=True
    )

