import json

from airflow.models import DAG

from airflow.providers.sqlite.operators.sqlite import SqlOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator

"""
Link a descargar paquetes de los operadores que no vienen por default
https://airflow.apache.org/docs/apache-airflow-providers/packages-ref.html
"""

import pandas as pd
from datetime import datetime
import json


def trasnforma_usuario_y_exporta(**context):
    task_instance = context['task_instance']
    usuarios = task_instance.xcom_pull(task_ids='extraer_usuario')
    if not len(usuarios) or 'results' not in usuarios[0]:
        raise ValueError('Usuario vacio')
    usuario = usuarios[0]['results'][0]
    usuario_procesado = pd.json_normalize({
        'nombre': usuario['name']['first'],
        'apellido': usuario['name']['last'],
        'pais': usuario['location']['country'],
        'usuario': usuario['login']['username'],
        'contrasena': usuario['login']['password'],
        'email': usuario['email']
    })
    usuario_procesado.to_csv('./usuario_procesado.csv', index=False)


default_args = {
    'name_dag': 'procesar_usuario',
    'schedule_interval': '@daily',
    'start_date': datetime(2021, 1, 1),
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

    # db_sqlite // conexion creada desde la interfaz
    crear_tabla = SqlOperator(
        task_id='crear_tabla',
        sqlite_conn_id='db_sqlite',
        sql="""
        CREATE TABLE IF NOT EXISTS usuarios(
            nombre TEXT NOT NULL,
            apellido TEXT NOT NULL,
            pais TEXT NOT NULL,
            usuario  TEXT NOT NULL,
            contrasena  TEXT NOT NULL,
            email  TEXT NOT NULL PRIMARY KEY,
        )
        """
    )

    # Verficando si el api esta disponible
    api_disponible = HttpSensor(
        task_id='api_disponible',
        http_conn_id='usuario_api',
        endpoint='api/'
    )

    # lambda response: json.loads(response.text) // permite trasnformar la response a Json
    extraer_usuario = SimpleHttpOperator(
        task_id='extraer_usuario',
        http_conn_id='usuario_api',
        endpoint='api/',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True
    )

    # Procesando el usuario
    procesa_usuario = PythonOperator(
        task_id='procesa_usuario',
        python_callable=trasnforma_usuario_y_exporta,
    )

    # Almacena en la BD
    almacenar_usuario = BashOperator(
        task_id='almacenar_usuario',
        bash_command = 'echo -e ".separator ","\n.import /path/usuario_procesado.csv usuarios | sqlite3 /path/database.db'
    )


# Orden de ejecucion

crear_tabla >> api_disponible >> extraer_usuario >> procesa_usuario >> almacenar_usuario