# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "c7d97355-8fbe-4167-a88e-09ddf8c7298e",
# META       "default_lakehouse_name": "lh_bronze",
# META       "default_lakehouse_workspace_id": "3425295b-1602-4706-a348-0d3b53450887",
# META       "known_lakehouses": [
# META         {
# META           "id": "c7d97355-8fbe-4167-a88e-09ddf8c7298e"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import pandas as pd
# Load data into pandas DataFrame from "/lakehouse/default/Files/raw_bronze/SalesLT/SalesLT/SalesOrderDetail/1900-01/SalesLT_SalesOrderDetail_1900-01-01_000000.parquet"
df = pd.read_parquet("/lakehouse/default/Files/raw_bronze/SalesLT/SalesLT/SalesOrderDetail/1900-01/SalesLT_SalesOrderDetail_1900-01-01_000000.parquet")
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
