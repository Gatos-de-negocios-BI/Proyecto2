import pandas as pd
import numpy as np
import inflect
from sklearn.preprocessing import MinMaxScaler
from sklearn.preprocessing import LabelEncoder as LE
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
    salud = pd.read_csv("/opt/airflow/data/Salud.csv", sep=";", decimal=",")
    salud = salud.drop(salud.index[0])
    salud = salud.drop('Dato Cualitativo', axis=1)
    salud = salud.dropna(how='any', axis=0)
    salud['Dato Numérico'] = salud['Dato Numérico'].str.replace('.','')
    salud['Dato Numérico'] = salud['Dato Numérico'].str.replace(',','.')
    salud['Dato Numérico'] = salud['Dato Numérico'].astype('float')

    municipios = salud.copy()[['Entidad', 'Código Entidad']]
    municipios = municipios.drop_duplicates(subset=['Código Entidad'])
    guardar_datos(municipios, "municipios")

    ## Dimension Subcategoria
    le = LE()
    le.fit(salud['Subcategoría'])
    salud['Subcategoría'] = le.transform(salud['Subcategoría'])
    SUBCAT_MAP = dict(zip(le.classes_, le.transform(le.classes_)))
    subcat = {"subcategoria": list(SUBCAT_MAP.keys()), "subcategoria_key": list(SUBCAT_MAP.values())}
    subcat_df = pd.DataFrame(subcat)
    guardar_datos(subcat_df, "subcategorias_salud")

    ## Dimension unidad de medida
    le = LE()
    le.fit(salud['Unidad de Medida'])
    salud['Unidad de Medida'] = le.transform(salud['Unidad de Medida'])
    UNIDAD_MAP = dict(zip(le.classes_, le.transform(le.classes_)))
    unidad = {"unidadDeMedida": list(UNIDAD_MAP.keys()), "unidadDeMedida_key": list(UNIDAD_MAP.values())}
    unidad_df = pd.DataFrame(unidad)
    guardar_datos(unidad_df, "unidades_salud")

    ## Dimension indicador
    le = LE()
    le.fit(salud['Indicador'])
    salud['Indicador'] = le.transform(salud['Indicador'])
    INDICADOR_MAP = dict(zip(le.classes_, le.transform(le.classes_)))
    indicador = {"indicador": list(INDICADOR_MAP.keys()), "indicador_key": list(INDICADOR_MAP.values())}
    indicador_df = pd.DataFrame(indicador)
    guardar_datos(indicador_df, "indicador_salud")
    salud = salud.dropna(how='any', axis=0)

    ## Dimension salud
    salud['salud_key'] = range(1, len(salud) + 1)
    column = 'Dato Numérico'
    salud[column] = MinMaxScaler().fit_transform(np.array(salud[column]).reshape(-1,1))


    guardar_datos(salud, "Salud_")