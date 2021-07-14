"""
Imports DAG
"""
from airflow.models import DAG
from airflow.utils import dates

from airflow.models import Variable

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
with DAG('dag_test_iget_value_global_variables',
         default_args=default_args,
         schedule_interval='@once') as dag:
    # Fromas de acceder a las variables globales configuradas en el dashboard
    variable_email = Variable.get('email')
    variable_json = Variable.get('json', deserialize_json=True)

