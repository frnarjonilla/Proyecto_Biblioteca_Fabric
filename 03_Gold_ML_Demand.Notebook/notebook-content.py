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

from pyspark.sql import functions as F

# 1. Cargamos la tabla Silver
df_silver = spark.read.table("checkouts_silver")

# 2. Adaptamos a los nombres de columna reales
# Vamos a crear una tabla Gold basada en 'temas' (que es la categoría)
df_gold = df_silver.select(
    F.col("temas").alias("Categoria"),
    F.col("mes").cast("int"),
    F.col("anio").cast("int"),
    F.col("total_prestamos").cast("int")
)

# 3. Ingeniería de Atributos (Añadimos DiaSemana si no existe)
# Como no tenemos la fecha completa "CheckoutDate", usaremos un valor por defecto 
# o simplemente entrenaremos con Mes y Año, que ya los tenemos.
df_gold = df_gold.withColumn("DiaSemana", F.lit(1)) # Valor temporal para que el código de ML no rompa

# Guardamos la tabla Gold
df_gold.write.mode("overwrite").format("delta").saveAsTable("Gold_Demanda_Libros")

print("✅ Tabla Gold creada con las columnas: Categoria, Mes, Anio, total_prestamos")
display(df_gold.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import mlflow
import mlflow.spark
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

# 1. Configurar el experimento en Fabric
mlflow.set_experiment("Prediccion_Demanda_Libros_Seattle")

with mlflow.start_run():
    # 2. Preparar los datos para el modelo
    # Usamos las columnas que existen en la tabla Gold actual
    # "DiaSemana" lo añadimos en la celda anterior como valor constante
    assembler = VectorAssembler(
        inputCols=["mes", "anio", "DiaSemana"], 
        outputCol="features",
        handleInvalid="skip"
    )
    
    df_ml = assembler.transform(df_gold)
    
    # Seleccionamos 'total_prestamos' como el valor a predecir (label)
    df_ml_final = df_ml.select("features", F.col("total_prestamos").alias("label"))
    
    # Dividimos en entrenamiento (80%) y prueba (20%)
    train_data, test_data = df_ml_final.randomSplit([0.8, 0.2], seed=42)

    # 3. Entrenar un modelo de Bosque Aleatorio (Random Forest)
    rf = RandomForestRegressor(featuresCol="features", labelCol="label")
    model = rf.fit(train_data)

    # 4. Evaluar el modelo
    predictions = model.transform(test_data)
    evaluator = RegressionEvaluator(labelCol="label", metricName="rmse")
    rmse = evaluator.evaluate(predictions)

    # 5. Registrar en MLflow
    mlflow.log_param("model_type", "RandomForest")
    mlflow.log_metric("rmse", rmse)
    mlflow.spark.log_model(model, "modelo_biblioteca_seattle")

    print(f"✅ ¡Entrenamiento completado!")
    print(f"📉 Error Medio Cuadrático (RMSE): {rmse}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
