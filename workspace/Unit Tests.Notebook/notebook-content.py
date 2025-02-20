# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "146dab8c-91ba-469f-ae7f-26700dfa5339",
# META       "default_lakehouse_name": "lh_silver",
# META       "default_lakehouse_workspace_id": "3425295b-1602-4706-a348-0d3b53450887",
# META       "known_lakehouses": [
# META         {
# META           "id": "146dab8c-91ba-469f-ae7f-26700dfa5339"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# Andy Jones created this notebook on 20/02/2025

# MARKDOWN ********************

# - https://onelake.dfs.fabric.microsoft.com/3425295b-1602-4706-a348-0d3b53450887/c7d97355-8fbe-4167-a88e-09ddf8c7298e/Files
# - Files
# - abfss://3425295b-1602-4706-a348-0d3b53450887@onelake.dfs.fabric.microsoft.com/c7d97355-8fbe-4167-a88e-09ddf8c7298e/Files

# MARKDOWN ********************

# - silver_saleslt_productcategory_new
# - https://onelake.dfs.fabric.microsoft.com/3425295b-1602-4706-a348-0d3b53450887/c7d97355-8fbe-4167-a88e-09ddf8c7298e/Tables/dbo/silver_saleslt_productcategory_new
# - Tables/dbo/silver_saleslt_productcategory_new
# - abfss://3425295b-1602-4706-a348-0d3b53450887@onelake.dfs.fabric.microsoft.com/c7d97355-8fbe-4167-a88e-09ddf8c7298e/Tables/dbo/silver_saleslt_productcategory_new

# CELL ********************

notebook_path = 'L1Transform-Generic-Fabric'
parameters = {
    'L1TransformInstanceID': 2,
    'L1TransformID': 21,
    'IngestID': 21,
    'CustomParameters': None,
    'InputRawFileSystem': 'abfss://3425295b-1602-4706-a348-0d3b53450887@onelake.dfs.fabric.microsoft.com/c7d97355-8fbe-4167-a88e-09ddf8c7298e/Files',
    'InputRawFileFolder': 'raw_bronze/SalesLT/SalesLT/ProductCategory/2025-01',
    'InputRawFile': 'SalesLT_ProductCategory_2025-01-24_004954.parquet',
    'InputRawFileDelimiter': None,
    'InputFileHeaderFlag': None,
    'OutputL1CurateFileSystem': None,
    'OutputL1CuratedFolder': None,
    'OutputL1CuratedFile': None,
    'OutputL1CuratedFileDelimiter': None,
    'OutputL1CuratedFileFormat': None,
    'OutputL1CuratedFileWriteMode': None,
    'OutputDWStagingTable': None,
    'LookupColumns': 'ProductCategoryID',
    'OutputDWTable': 'silver.saleslt_productcategory_003',
    'OutputDWTableWriteMode': 'append',
    'ReRunL1TransformFlag': 'false',
    'WatermarkColName': 'ModifiedDate'
}
mssparkutils.notebook.run(notebook_path, 3600, parameters)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Update one row to test an upsert

# CELL ********************

from delta.tables import DeltaTable

# Define the path to the Delta table
delta_table_path = "Tables/dbo/silver_saleslt_productcategory_new"

# Load the Delta table
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Perform the update
delta_table.update(
    condition="ProductCategoryID = 1",
    set={"Name": "'Balls'"}
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_bronze.dbo.silver_saleslt_productcategory_new LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# Test a SP write to the gold tier
