from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, to_date, count,
    sum as suma, avg, desc, month
)

# 1. Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("AnalisisMultasSIMIT2025") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Leer los datos almacenados en HDFS
ruta = "hdfs://localhost:9000/user/hadoop/simit/72nf-y4v3.csv"

df = spark.read.csv(ruta, header=True, inferSchema=False)

print("\n========== 1. DATOS ORIGINALES ==========")
print("Total de registros:", df.count())
print("Columnas:", df.columns)

# 3. Limpiar textos y convertir tipos de datos
df_limpio = df.select(
    trim(col("vigencia")).cast("integer").alias("vigencia"),
    upper(trim(col("placa"))).alias("placa"),
    to_date(trim(col("fecha_multa")), "dd/MM/yyyy").alias("fecha_multa"),
    trim(col("valor_multa")).cast("double").alias("valor_multa"),
    upper(trim(col("departamento"))).alias("departamento"),
    upper(trim(col("ciudad"))).alias("ciudad"),
    upper(trim(col("pagado_si_no"))).alias("pagado_si_no")
)

# 4. Revisar valores nulos y conversiones fallidas
print("\n========== 2. CALIDAD DE LOS DATOS ==========")

for columna in df_limpio.columns:
    errores = df_limpio.filter(col(columna).isNull()).count()
    print(f"{columna}: {errores} valores nulos o conversiones fallidas")

# 5. Analizar los registros de 2025
df_2025 = df_limpio.filter(
    (col("vigencia") == 2025) &
    col("fecha_multa").isNotNull() &
    col("valor_multa").isNotNull()
)

print("\n========== 3. RESUMEN GENERAL ==========")
df_2025.select(
    count("*").alias("Total_registros_validos"),
    suma("valor_multa").alias("Valor_total_multas"),
    avg("valor_multa").alias("Promedio_multa")
).show(truncate=False)

# 6. Multas pagadas y pendientes
print("\n========== 4. ESTADO DE PAGO ==========")
df_2025.groupBy("pagado_si_no") \
    .agg(
        count("*").alias("Cantidad"),
        suma("valor_multa").alias("Valor_total")
    ) \
    .orderBy(desc("Cantidad")) \
    .show(truncate=False)

# 7. Departamentos con más multas
print("\n========== 5. MULTAS POR DEPARTAMENTO ==========")
df_2025.groupBy("departamento") \
    .agg(
        count("*").alias("Cantidad_multas"),
        suma("valor_multa").alias("Valor_total")
    ) \
    .orderBy(desc("Cantidad_multas")) \
    .show(10, truncate=False)

# 8. Ciudades con más multas
print("\n========== 6. MULTAS POR CIUDAD ==========")
df_2025.groupBy("ciudad") \
    .agg(count("*").alias("Cantidad_multas")) \
    .orderBy(desc("Cantidad_multas")) \
    .show(10, truncate=False)

# 9. Multas por mes
print("\n========== 7. MULTAS POR MES ==========")
df_2025.groupBy(month("fecha_multa").alias("Mes")) \
    .agg(
        count("*").alias("Cantidad_multas"),
        suma("valor_multa").alias("Valor_total")
    ) \
    .orderBy("Mes") \
    .show(12, truncate=False)

# 10. Revisar registros duplicados exactos
print("\n========== 8. REGISTROS DUPLICADOS ==========")
duplicados = df_2025.count() - df_2025.dropDuplicates().count()
print("Cantidad de registros duplicados exactos:", duplicados)

print("\n========== ANÁLISIS FINALIZADO ==========")

spark.stop()
