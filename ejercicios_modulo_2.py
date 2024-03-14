import os
import sys

from pyspark.sql import SparkSession

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

# Crea la sesión de Spark
spark = SparkSession.builder.appName("Modulo2A").getOrCreate()

# Ruta del fichero de texto
ruta_fichero = "resources/relato.txt"

# Carga el fichero como un RDD
rdd = spark.sparkContext.textFile(ruta_fichero)

# Cuenta el número de líneas
num_lineas = rdd.count()

# Muestra el resultado
print(f"El número de líneas en el fichero {ruta_fichero} es: {num_lineas}\n")

# Aplicar la función map para convertir cada línea a minúsculas
rdd_minusculas = rdd.map(lambda x: x.lower())

# Mostrar las primeras 5 líneas del nuevo RDD
print("Ejemplo de líneas en minúsculas:")
for linea in rdd_minusculas.take(5):
    print(f"linea: {linea}\n")

# Aplicar flatMap para dividir cada línea en palabras
rdd_palabras = rdd.flatMap(lambda x: x.split())

# Mostrar las primeras 10 palabras del nuevo RDD
print("Ejemplo de palabras:")
for palabra in rdd_palabras.take(10):
    print(palabra)

# Definir la palabra específica a filtrar
palabra_filtrar = "luz"

# Aplicar filter para filtrar las líneas que contienen la palabra específica
rdd_filtrado = rdd.filter(lambda x: palabra_filtrar in x)

# Mostrar las líneas filtradas
print(f"Líneas que contienen la palabra {palabra_filtrar}:")
for linea in rdd_filtrado.collect():
    print(linea)

# Obtener un nuevo RDD con palabras únicas
rdd_palabras_unicas = rdd_palabras.distinct()

# Mostrar las primeras palabras únicas del nuevo RDD
print("Palabras únicas:")
for palabra in rdd_palabras_unicas.take(20):
    print(palabra)

# Obtener una muestra aleatoria de palabras
fraccion_muestra = 0.1  # Fracción de elementos a incluir en la muestra
con_reemplazo = False  # Si se permite el muestreo con reemplazo

rdd_muestra = rdd_palabras.sample(withReplacement=con_reemplazo, fraction=fraccion_muestra)

# Mostrar las palabras de la muestra
print("Muestra aleatoria de palabras:")
for palabra in rdd_muestra.collect():
    print(palabra)
