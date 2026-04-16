# 📚 Sistema Predictivo de Biblioteca - Microsoft Fabric & Airflow

Este proyecto implementa una solución de **End-to-End Data Engineering** utilizando **Microsoft Fabric**. El objetivo es procesar el dataset masivo de la Biblioteca Pública de Seattle para predecir la demanda de inventario mediante Machine Learning, todo bajo un flujo profesional de **CI/CD**.

## 🛠️ Stack Tecnológico
* **Plataforma:** Microsoft Fabric (Trial Capacity)
* **Orquestación:** Apache Airflow (Data Workflows nativos)
* **Procesamiento:** PySpark (Notebooks)
* **Almacenamiento:** Lakehouse (Arquitectura Medallion - Delta Lake)
* **Control de Versiones:** GitHub + Fabric Git Integration
* **CI/CD:** Fabric Deployment Pipelines (Dev -> Test -> Prod)

## 🏗️ Arquitectura de Datos (Medallion)
1.  **Bronze (Raw):** Ingestión de CSVs originales desde Seattle Open Data.
2.  **Silver (Clean):** Limpieza de esquemas, normalización de títulos y autores con Spark.
3.  **Gold (Analytics/ML):** Tablas agregadas y entrenamiento de modelos de regresión para predecir préstamos.

## 🔄 Flujo de Trabajo (DAG de Airflow)
El pipeline está orquestado mediante un DAG que gestiona:
- Verificación de nuevos datos.
- Ejecución secuencial de Notebooks de transformación.
- Re-entrenamiento del modelo de ML en caso de deriva de datos.

## 🚀 Implementación de CI/CD
El proyecto utiliza un sistema de dos entornos:
- **`Biblioteca_Ventas_DEV`**: Espacio de desarrollo conectado a este repositorio.
- **`Biblioteca_Ventas_TEST`**: Espacio de pruebas conectado a Biblioteca_Ventas_Dev.
- **`Biblioteca_Ventas_PROD`**: Espacio de producción actualizado mediante **Deployment Pipelines**, asegurando que solo el código probado llegue al usuario final.

---

## 🚀 Resultados del Proyecto
- **Arquitectura:** Medallion (Bronze -> Silver -> Gold).
- **Modelo:** RandomForestRegressor entrenado para predecir demanda de libros.
- **Rendimiento:** Se alcanzó un RMSE de 8.06, demostrando la viabilidad de la predicción mensual.
- **Gobernanza:** Experimentos registrados y versionados mediante MLflow.
