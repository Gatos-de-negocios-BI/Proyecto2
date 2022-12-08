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
    anio = [item for item in range(1900, 2030+1)]
    anio_word = []
    p = inflect.engine()
    for year in anio:
        anio_word.append(p.number_to_words(year))
    anio_structure = {'anio':anio, 'descripcion':anio_word}
    anio_df = pd.DataFrame(anio_structure)
    guardar_datos(anio_df, "anio")

    ## Dimension municipio
    demografia = pd.read_csv("/opt/airflow/data/DemografiaYPoblacion.csv", sep=";", decimal=",")
    demografia = demografia.drop(demografia.index[0])
    municipios = demografia.copy()[['Entidad', 'Código Entidad']]
    municipios = municipios.drop_duplicates(subset=['Código Entidad'])
    guardar_datos(municipios, "municipios")

    ## Dimension Subcategoria
    subcategoria = {"subcategoria_key": [1,2], "subcategoria":['Población de hombres', 'Población de mujeres']}
    subcategoria_df = pd.DataFrame(subcategoria)
    guardar_datos(subcategoria_df, "subcategoria")

    ## Dimension demografia y poblacion
    demografia = pd.read_csv("/opt/airflow/data/DemografiaYPoblacion.csv", sep=";", decimal=",")
    demografia = demografia.drop(demografia.index[0])
    demografia = demografia.drop('Dato Cualitativo', axis=1)
    demografia = demografia.dropna(how='any', axis=0)
    demografia['Dato Numérico'] = demografia['Dato Numérico'].str.replace('.','')
    demografia['Dato Numérico'] = demografia['Dato Numérico'].str.replace(',','.')
    demografia['Dato Numérico'] = demografia['Dato Numérico'].astype('float')
    demografia = demografia[['Código Entidad', 'Entidad', 'Subcategoría', 'Dato Numérico', 'Año']].groupby(['Código Entidad', 'Entidad', 'Subcategoría', 'Año']).sum().reset_index()
    demografia =demografia.loc[(demografia['Subcategoría'].str.contains('Población de hombres') )| (demografia['Subcategoría'].str.contains('Población de mujeres'))]
    demografia['Subcategoría'] = demografia['Subcategoría'].str.replace('Población de hombres', '1')
    demografia['Subcategoría'] = demografia['Subcategoría'].str.replace('Población de mujeres', '2')
    demografia['Subcategoría'] = demografia['Subcategoría'].astype('int')
    demografia['demografia_key'] = range(1, len(demografia) + 1)
    column = 'Dato Numérico'
    demografia[column] = MinMaxScaler().fit_transform(np.array(demografia[column]).reshape(-1,1))
    
    

    guardar_datos(demografia, "DemografiaYPoblacion_")