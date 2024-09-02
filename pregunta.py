"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.


"""

import re
import pandas as pd


def ingest_data():

    #
    # Inserte su código aquí
    #

    with open("clusters_report.txt", "r") as file:
        lineas_report = file.readlines()

        # encabezados
        lineas_report[0] = re.sub(r"\s{2,}", "-", lineas_report[0]).strip().split("-")
        lineas_report[1] = re.sub(r"\s{2,}", "-", lineas_report[1]).strip().split("-")
        lineas_report[0].pop(), lineas_report[1].pop(0)

        # diccionario de datos para el DataFrame
        dataset = {
            lineas_report[0][0]: [],
            lineas_report[0][1] + " " + lineas_report[1][0]: [],
            lineas_report[0][2] + " " + lineas_report[1][1]: [],
            lineas_report[0][3]: [],
        }


        dataset = {col.lower().replace(' ', '_'): col_data for col, col_data in dataset.items()}

        # procesamiento de las líneas con los datos de los clusters
        for idx in range(2, len(lineas_report)):
            
            # reemplazo de múltiples espacios en blanco y filtrado de elementos vacíos
            lineas_report[idx] = re.sub(r"\s{2,}", ".", lineas_report[idx]).strip().split(".")
            lineas_report[idx] = list(filter(lambda elem: elem, lineas_report[idx]))

            # isnumeric
            if lineas_report[idx] and lineas_report[idx][0].isnumeric():
                dataset["cluster"].append(int(lineas_report[idx][0]))
                dataset["cantidad_de_palabras_clave"].append(int(lineas_report[idx][1]))
                dataset["porcentaje_de_palabras_clave"].append(float(lineas_report[idx][2][:-2].replace(",", ".")))
                dataset["principales_palabras_clave"].append(" ".join(lineas_report[idx][3:]))

            # si no es un nuevo registro, se continúa con las palabras clave del registro anterior
            elif dataset["principales_palabras_clave"]:
                last_keywords = dataset["principales_palabras_clave"].pop() + " " + " ".join(lineas_report[idx])
                dataset["principales_palabras_clave"].append(last_keywords.strip())

        df = pd.DataFrame(dataset)
        return df

#df = ingest_data()
#print(df)