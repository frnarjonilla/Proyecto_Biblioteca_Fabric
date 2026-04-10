# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2498094b-6b8b-43dd-88f9-8575cb72da2b",
# META       "default_lakehouse_name": "LH_Biblioteca_Seattle",
# META       "default_lakehouse_workspace_id": "89c188da-fb80-4076-9a89-31304909b91d",
# META       "known_lakehouses": [
# META         {
# META           "id": "2498094b-6b8b-43dd-88f9-8575cb72da2b"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

from pyspark.sql.functions import col, current_timestamp, upper, trim

# 1. Leer el CSV desde la carpeta Bronce
path_bronce = "Files/Bronze/checkouts_biblioteca_raw.csv"

df_raw = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load(path_bronce)

# 2. Transformaciones (Capa Plata)
# - Limpiamos espacios en blanco en títulos y autores
# - Pasamos los títulos a MAYÚSCULAS para estandarizar
# - Filtramos registros donde el título sea nulo
df_silver = df_raw.select(
    trim(upper(col("title"))).alias("titulo"),
    trim(col("creator")).alias("autor"),
    col("subjects").alias("temas"),
    col("checkoutmonth").alias("mes"),
    col("checkoutyear").alias("anio"),
    col("checkouts").alias("total_prestamos")
).filter(col("titulo").isNotNull())

df_silver = df_silver.na.fill({"autor": "Desconocido", "temas": "Sin categoría"})

# Añadimos columna de auditoría
df_silver = df_silver.withColumn("fecha_procesado", current_timestamp())

# 3. Guardar como TABLA DELTA (La magia de Fabric)
# Esto crea una tabla real que podrás consultar con SQL
df_silver.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("checkouts_silver")

print("✅ Capa Plata creada con éxito. Tabla 'checkouts_silver' lista en el Lakehouse.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
