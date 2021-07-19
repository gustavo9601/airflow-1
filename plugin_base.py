from airflow.plugin_manager import AirflowPlugin

"""
Schema Base of Plugin
"""
class SqlPluginTest(AirflowPlugin):
    name = 'SqlPluginTest'  # Nombre que tomara como base airflow
    operators = []
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []