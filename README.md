# Análisis Batch de multas del SIMIT con Apache Spark

## Descripción del proyecto

En el análisis Batch de los registros de multas del SIMIT vigencia 2025, se utiliza Apache Spark para procesar los datos almacenados en Hadoop HDFS.
Las columnas utilizadas en el conjunto de datos son: vigencia, placa, fecha de la multa, valor, departamento, ciudad y estado de pago.

## Tecnologías utilizadas
- **Apache Hadoop HDFS:** almacenamiento del archivo CSV.
- **Apache Spark y PySpark:** procesamiento y análisis de los datos.
- **Python:** desarrollo del código de procesamiento Batch.

## Procesamiento realizado
El programa `simit_batch.py` realiza las siguientes operaciones:

1. Lee el archivo CSV desde HDFS.
2. Limpia espacios y estandariza los campos de texto.
3. Convierte la vigencia y el valor de las multas a tipos numéricos, y la fecha a tipo fecha.
4. Revisa valores nulos y posibles errores de conversión.
5. Calcula la cantidad de registros, el valor total y el promedio de las multas.
6. Agrupa los registros por estado de pago, departamento, ciudad y mes.
7. Identifica registros duplicados exactos.

## Resultados obtenidos
En la ejecución realizada se obtuvieron los siguientes resultados:

- Total de registros analizados: **60.401**.
- Valor total registrado de las multas: **$45.875.750.940**.
- Valor promedio por registro: aproximadamente **$759.519,73**.
- Registros marcados como pagados: **33.667**.
- Registros marcados como no pagados: **26.734**.
- Registros duplicados exactos identificados: **129**.

## Requisitos de ejecución
Para ejecutar el programa se requiere:

- Python compatible con la versión de PySpark instalada.
- Apache Spark.
- Hadoop HDFS en funcionamiento.
- El archivo CSV disponible en la ruta HDFS configurada en el código.

## Ejecución
Con Hadoop iniciado y el archivo CSV cargado en HDFS, ejecutar:
```bash
/opt/spark/bin/spark-submit /home/hadoop/simit_batch.py
```
## Archivo principal
- `simit_batch.py`: código fuente del análisis Batch desarrollado con PySpark.
