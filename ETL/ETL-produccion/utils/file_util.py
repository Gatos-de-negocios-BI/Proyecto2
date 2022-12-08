import pandas as pd
import numpy as np
import inflect
from sklearn.preprocessing import MinMaxScaler
import os

def cargar_datos(name):
    df = pd.read_csv("/opt/airflow/data/" + name + ".csv", sep=',', encoding = 'latin1', index_col=False)
    return df

def guardar_datos(df, nombre):
    df.to_csv("/opt/airflow/data/" + nombre + ".csv" , encoding = 'latin1', sep=',', index=False)  
    
def procesar_datos():
    
    ## Dimension anio
    anio = [item for item in range(1900, 2022+1)]
    anio_word = []
    p = inflect.engine()
    for year in anio:
        anio_word.append(p.number_to_words(year))
    anio_structure = {'anio':anio, 'descripcion':anio_word}
    anio_df = pd.DataFrame(anio_structure)
    guardar_datos(anio_df, "anio")

    ## Dimension municipio
    produccionInfo = pd.read_json("/opt/airflow/data/mineriaInfo.json")
    municipios = produccionInfo.copy()[['municipio_productor', 'codigo_dane']]
    municipios = municipios.drop_duplicates(subset=['codigo_dane'])
    guardar_datos(municipios, "municipios")
    
    ## Dimension produccionMineraDeMetales
    produccionInfo = produccionInfo.dropna(how='any', axis=0)
    produccionInfo = produccionInfo[produccionInfo['unidad_medida'].str.fullmatch('GRAMOS')]
    produccionInfo= produccionInfo[['municipio_productor', 'a_o_produccion', 'valor_contraprestacion', 'cantidad_producci_n', 'unidad_medida', 'codigo_dane']].groupby(['municipio_productor', 'a_o_produccion', 'unidad_medida', 'codigo_dane']).sum().reset_index()    
    produccionInfo['mineria_key'] = range(1, len(produccionInfo) + 1)
    column = 'cantidad_producci_n'
    produccionInfo[column] = MinMaxScaler().fit_transform(np.array(produccionInfo[column]).reshape(-1,1))
    guardar_datos(produccionInfo, "produccionMineraDeMetales")