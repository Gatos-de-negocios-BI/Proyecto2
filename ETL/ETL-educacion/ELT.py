# Utilidades de airflow
from airflow.models import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.task_group import TaskGroup

# Utilidades de python
from datetime  import datetime

# Funciones ETL
from utils.crear_tablas import crear_tablas
from utils.file_util import procesar_datos
from utils.insert_queries import *

default_args= {
    'owner': 'Estudiante',
    'email_on_failure': False,
    'email': ['estudiante@uniandes.edu.co'],
    'start_date': datetime(2022, 5, 5) # inicio de ejecución
}

with DAG(
    "ETL",
    description='ETL',
    schedule_interval='@daily', # ejecución diaría del DAG
    default_args=default_args, 
    catchup=False) as dag:

    # task: 1 - preprocesamiento
    preprocesamiento = PythonOperator(
        task_id='preprocesamiento',
        python_callable=procesar_datos
    )

    # task: 2 crear las tablas en la base de datos postgres
    crear_tablas_db = PostgresOperator(
    task_id="crear_tablas_en_postgres",
    postgres_conn_id="educacion_etl", # Nótese que es el mismo ID definido en la conexión Postgres de la interfaz de Airflow
    sql= crear_tablas()
    )

    # task: 3 poblar las tablas de dimensiones en la base de datos
    with TaskGroup('poblar_tablas') as poblar_tablas_dimensiones:

        # task: 3.1 poblar tabla anio
        poblar_city = PostgresOperator(
            task_id="poblar_anio",
            postgres_conn_id='educacion_etl',
            sql=insert_query_anio(csv_path = "anio")
        )

        # task: 3.2 poblar tabla municipio
        poblar_customer = PostgresOperator(
            task_id="poblar_municipio",
            postgres_conn_id='educacion_etl',
            sql=insert_query_municipio(csv_path ="municipios")
        )

        # task: 3.3 poblar tabla subcategoria
        poblar_customer = PostgresOperator(
            task_id="poblar_subcategoria",
            postgres_conn_id='educacion_etl',
            sql=insert_query_subcategoria(csv_path ="subcategorias_educacion")
        )

        # task: 3.4 poblar tabla unidad de medida
        poblar_customer = PostgresOperator(
            task_id="poblar_unidadDeMedida",
            postgres_conn_id='educacion_etl',
            sql=insert_query_unidadDeMedida(csv_path ="unidades_educacion")
        )

        # task: 3.5 poblar tabla indicador
        poblar_customer = PostgresOperator(
            task_id="poblar_indicador",
            postgres_conn_id='educacion_etl',
            sql=insert_query_indicador(csv_path ="indicador_educacion")
        )


    # task: 4 poblar la tabla de hechos
    poblar_fact_order = PostgresOperator(
            task_id="construir_educacion",
            postgres_conn_id='educacion_etl',
            sql=insert_query_educacion(csv_path = "Educacion_")
    )

    # flujo de ejecución de las tareas  
    preprocesamiento >> crear_tablas_db >> poblar_tablas_dimensiones >> poblar_fact_order