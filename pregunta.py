"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""

import pandas as pd


def ingest_data():
    df = pd.read_fwf("clusters_report.txt", widths=[9, 16, 16, 77])
    df.columns = [col.strip().replace(" ", "_").lower() for col in df.columns]
    df.rename(columns = {'cantidad_de':'cantidad_de_palabras_clave', 'porcentaje_de':'porcentaje_de_palabras_clave'}, inplace = True)
    df.drop([0,1], inplace=True)
    df.cluster.ffill(inplace=True)
    df['principales_palabras_clave'] = df['principales_palabras_clave'].groupby(df['cluster']).transform(lambda x: ' '.join(x))
    df.cantidad_de_palabras_clave.ffill(inplace=True)
    df.porcentaje_de_palabras_clave.ffill(inplace=True)
    df.drop_duplicates(inplace=True)
    df.reset_index(inplace=True, drop=True)
    df.cluster = df.cluster.astype(int)
    df.cantidad_de_palabras_clave = df.cantidad_de_palabras_clave.astype(int)
    df.porcentaje_de_palabras_clave = df.porcentaje_de_palabras_clave.str.replace('%','').str.replace(',','.').astype(float)
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace('    ', ' ').str.replace('   ', ' ').str.replace('  ', ' ').str.replace(',,', ',').str.replace('.', '')
    return df
