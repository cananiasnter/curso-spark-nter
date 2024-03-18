import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, min, avg, count


os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# ***************************
# ********* PARTE A *********
# ***************************

def print_rows(rows):
    for row in rows:
        print(row)

# Crea la sesión de Spark
spark = SparkSession.builder.appName("Capitulo2").getOrCreate()

# Ruta del fichero de texto
ruta_fichero = "resources/el_quijote.txt"

# Carga el fichero como un RDD
df = spark.read.text(ruta_fichero)

# Cuenta el número de líneas
num_lineas = df.count()

# Muestra el resultado
print(f"El número de líneas en el fichero {ruta_fichero} es: {num_lineas}\n")

# Muestra las primeras 20 filas truncando las columnas
print("\n###### Método show - default ######")
df.show()

# Muestra las primeras 5 filas truncando las columnas
print("\n###### Método show - 5 filas ######")
df.show(5)

# Muestra las primeras 5 filas sin truncar las columnas
print("\n###### Método show - sin truncate ######")
df.show(5, truncate=False)

# Devuelve la primera fila como un objeto Row
head_row = df.head()
print("\n###### Método head - default ######")
print(head_row)

# Devuelve las primeras 3 filas como una lista de objetos Row
head3_rows = df.head(3)
print("\n###### Método head - 3 filas ######")
print_rows(head3_rows)

# Devuelve las primeras 3 filas como una lista de objetos Row
take3_rows = df.take(3)
print("\n###### Método take - 3 filas ######")
print_rows(take3_rows)

# Devuelve la primera fila como un objeto Row
first_row = df.first()
print("\n###### Método first ######")
print(first_row)

print("\n\n################")
print("Fin de la parte A.")
print("Presiona enter para continuar...")
input()

# ***************************
# ********* PARTE B *********
# ***************************

# La primera parte del código pertenece al ejemplo extraido del libro
# get the M&M data set file name
mnm_file = "resources/mnm_dataset.csv"

# read the file into a Spark DataFrame
mnm_df = (spark.read.format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .load(mnm_file))
mnm_df.show(n=5, truncate=False)

# aggregate count of all colors and groupBy state and color
# orderBy descending order
count_mnm_df = (mnm_df.select("State", "Color", "Count")
                .groupBy("State", "Color")
                .sum("Count")
                .orderBy("sum(Count)", ascending=False))

# show all the resulting aggregation for all the dates and colors
count_mnm_df.show(n=60, truncate=False)
print("Total Rows = %d" % (count_mnm_df.count()))

# find the aggregate count for California by filtering
ca_count_mnm_df = (mnm_df.select("*")
                    .where(mnm_df.State == 'CA')
                    .groupBy("State", "Color")
                    .sum("Count")
                    .orderBy("sum(Count)", ascending=False))

# show the resulting aggregation for California
ca_count_mnm_df.show(n=10, truncate=False)

print("\n###### i. Agregación con max en orden descencente ######")
max_value = mnm_df.select(max(col("Count")).alias("Max_Count")).orderBy(col("Max_Count").desc()).first()
print(max_value)

# Lista de estados que queremos incluir
estados = ['CA', 'NY', 'TX']

# Consulta para los estados especificados
estado_count_mnm_df = (mnm_df.select("*")
                       .where((mnm_df.State == estados[0]) | 
                              (mnm_df.State == estados[1]) | 
                              (mnm_df.State == estados[2]))
                       .groupBy("State", "Color")
                       .sum("Count")
                       .orderBy("sum(Count)", ascending=False))

print("\n###### ii. Incluyendo más estados ######")
estado_count_mnm_df.show()

# Calcular el máximo, mínimo, promedio y conteo de la columna 'Count'
result = mnm_df.agg(max("Count").alias("Max_Count"),
                    min("Count").alias("Min_Count"),
                    avg("Count").alias("Avg_Count"),
                    count("Count").alias("Total_Count"))

print("\n###### iii. Cálculo en una misma operación de max, min, avg y count ######")
result.show()
