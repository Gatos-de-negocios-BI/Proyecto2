from utils.file_util import cargar_datos
import traceback

# anio insertion
def insert_query_anio(**kwargs):
    
    insert = f"INSERT INTO anio (anio_key,descripcion) VALUES "
    insertQuery = ""
    # Es necesario colocar este try porque airflow comprueba el funcionamiento de las tareas en paralelo y al correr el DAG no existe el archivo dimension_city. Deben colocar try y except en todas las funciones de insert
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"({row.anio},\'{row.descripcion}\');\n"
        return insertQuery
    except:
        return ""

# municipio insertion
def insert_query_municipio(**kwargs):
    insert = f"INSERT INTO municipio (municipio_key,nombre) VALUES "
    insertQuery = ""
    # Es necesario colocar este try porque airflow comprueba el funcionamiento de las tareas en paralelo y al correr el DAG no existe el archivo dimension_city. Deben colocar try y except en todas las funciones de insert
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"({row.codigo_dane},\'{row.municipio_productor}\');\n"
        return insertQuery
    except:
        return traceback.format_list()

# produccionMineraDeMetales insertion
def insert_query_produccionMineraDeMetales(**kwargs):
    insert = f"INSERT INTO produccionMineraDeMetales (Order_Key,municipio_key, anio_key, produccion_total) VALUES "
    insertQuery = ""
    # Es necesario colocar este try porque airflow comprueba el funcionamiento de las tareas en paralelo y al correr el DAG no existe el archivo dimension_city. Deben colocar try y except en todas las funciones de insert
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"({row.mineria_key},{row.codigo_dane},{row.a_o_produccion}, {row.cantidad_producci_n});\n"
        return insertQuery
    except:
        return traceback.format_list()