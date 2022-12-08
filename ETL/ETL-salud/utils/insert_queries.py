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
            insertQuery += insert + f"({row['Código Entidad']},\'{row.Entidad}\');\n"
        return insertQuery
    except:
        return ""

# subcategoria insertion
def insert_query_subcategoria(**kwargs):
    insert = f"INSERT INTO subcategoria (subcategoria_key,subcategoria) VALUES "
    insertQuery = ""
    # Es necesario colocar este try porque airflow comprueba el funcionamiento de las tareas en paralelo y al correr el DAG no existe el archivo dimension_city. Deben colocar try y except en todas las funciones de insert
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"({row.subcategoria_key},\'{row.subcategoria}\');\n"
        return insertQuery
    except:
        return ""

# indicador insertion
def insert_query_indicador(**kwargs):
    insert = f"INSERT INTO indicador (indicador_key,indicador) VALUES "
    insertQuery = ""
    # Es necesario colocar este try porque airflow comprueba el funcionamiento de las tareas en paralelo y al correr el DAG no existe el archivo dimension_city. Deben colocar try y except en todas las funciones de insert
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"({row.indicador_key},\'{row.indicador}\');\n"
        return insertQuery
    except:
        return ""

# unidadDeMedida insertion
def insert_query_unidadDeMedida(**kwargs):
    insert = f"INSERT INTO unidadDeMedida (unidadDeMedida_key,unidadDeMedida) VALUES "
    insertQuery = ""
    # Es necesario colocar este try porque airflow comprueba el funcionamiento de las tareas en paralelo y al correr el DAG no existe el archivo dimension_city. Deben colocar try y except en todas las funciones de insert
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"({row.unidadDeMedida_key},\'{row.unidadDeMedida}\');\n"
        return insertQuery
    except:
        return ""

# educacion insertion
def insert_query_salud(**kwargs):
    insert = f"INSERT INTO salud (salud_Key,Subcategoria_Key,UnidadDeMedida_Key, Indicador_Key, Anio_Key, Municipio_Key, datoNumerico) VALUES "
    insertQuery = ""
    # Es necesario colocar este try porque airflow comprueba el funcionamiento de las tareas en paralelo y al correr el DAG no existe el archivo dimension_city. Deben colocar try y except en todas las funciones de insert
    try:
        dataframe =cargar_datos(kwargs['csv_path'])
        for index, row in dataframe.iterrows():
            insertQuery += insert + f"({row.salud_key},{row.Subcategoría},{row['Unidad de Medida']},{row.Indicador},{row.Año}, {row['Código Entidad']},{row['Dato Numérico']});\n"
        return insertQuery
    except:
        return ""