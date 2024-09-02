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

    #
    # Inserte su código aquí
    #

    with open('clusters_report.txt') as file:
        lines = file.readlines()

    # filtrar y limpiar las líneas de texto
    clean_lines = [line.rstrip() for line in lines if line.strip()]

    data = []

    # procesar las líneas correspondientes a cada cluster
    i = 4  # comienzan los datos de clusters
    while i < len(clean_lines):
        line = clean_lines[i]

        # verifica si la línea comienza con un número, indicando un nuevo cluster
        if line.split()[0].isdigit():

            # extraer datos de la primera línea del cluster
            parts = line.split()
            cluster_id = int(parts[0])
            cantidad_palabras = int(parts[1])
            porcentaje_palabras = float(parts[2].replace(',', '.').replace('%', ''))
            
            # extraer y limpiar las palabras clave
            palabras_clave = " ".join(line[30:].split())

            # continuar hasta que las palabras clave se terminen
            i += 1
            while i < len(clean_lines) and not clean_lines[i].split()[0].isdigit():
                palabras_clave += " " + " ".join(clean_lines[i].strip().split())
                i += 1

            palabras_clave = palabras_clave.replace(' ,', ',').replace(', ', ',').replace(',', ', ').strip()

            data.append([cluster_id, cantidad_palabras, porcentaje_palabras, palabras_clave])
        else:
            i += 1
    
    df = pd.DataFrame(data, columns=["cluster", "cantidad_de_palabras_clave", "porcentaje_de_palabras_clave", "principales_palabras_clave"])

    return df

#df = ingest_data()
#print(df)