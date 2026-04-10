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

import requests

# 1. Configuración
DATASET_ID = "tmmm-ytt6"
url = f"https://data.seattle.gov/resource/{DATASET_ID}.csv?$limit=50000"
target_path = "/lakehouse/default/Files/Bronze/checkouts_biblioteca_raw.csv"

print(f"🤖 Iniciando descarga automática del dataset {DATASET_ID}...")

# 2. Proceso de descarga
try:
    response = requests.get(url, timeout=60)
    
    if response.status_code == 200:
        # Abrimos el archivo en modo escritura binaria ('wb')
        with open(target_path, "wb") as f:
            f.write(response.content)
        print("✅ ¡Éxito! El archivo se ha guardado en Files/Bronze.")
    else:
        print(f"❌ Error: La API respondió con código {response.status_code}")

except Exception as e:
    print(f"❌ Error de conexión: {e}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
