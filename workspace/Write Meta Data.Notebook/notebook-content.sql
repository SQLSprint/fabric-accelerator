-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {}
-- META }

-- CELL ********************

USE [controlDB-98393d97-fe48-41bc-b366-5ec5b4d7941e]
GO

UPDATE [ELT].[L2TransformDefinition]
   SET [IngestID] = 50
      ,[L1TransformID] = 50
      ,[ComputePath] = null
      ,[ComputeName] = '[gold].[create_dim_productcategory]' 
      ,[CustomParameters] = null 
      ,[InputType] = 'Curated' 
      ,[InputFileSystem] = 'Tables' 
      ,[InputFileFolder] = 'silver.saleslt_productcategory' 
      ,[InputFile] = null 
      ,[InputFileDelimiter] = null 
      ,[InputFileHeaderFlag] = null 
      ,[InputDWTable] = 'silver.saleslt_productcategory' 
      ,[WatermarkColName] = 'ModifiedDate'
      ,[LastDeltaDate] = null 
      ,[LastDeltaNumber] = null 
      ,[MaxIntervalMinutes] = null 
      ,[MaxIntervalNumber] = null 
      ,[MaxRetries] = 3 
      ,[OutputL2CurateFileSystem] = 'Tables' 
      ,[OutputL2CuratedFolder] = 'gold' 
      ,[OutputL2CuratedFile] = 'dim_productcategory' 
      ,[OutputL2CuratedFileDelimiter] = null 
      ,[OutputL2CuratedFileFormat] = null 
      ,[OutputL2CuratedFileWriteMode] = null 
      ,[OutputDWStagingTable] = null 
      ,[LookupColumns] = null 
      ,[OutputDWTable] = '[gold].[create_dim_productcategory]' 
      ,[OutputDWTableWriteMode] = 'append' 
      ,[ActiveFlag] = 1 
      ,[RunSequence] = 100 
      ,[CreatedBy] = 'andy.jones@clouddirectdemo.net' 
      ,[ModifiedBy] = 'andy.jones@clouddirectdemo.net' 
      ,[ModifiedTimestamp] = CURRENT_TIMESTAMP 
 WHERE [L2TransformID] = 55
GO




-- METADATA ********************

-- META {
-- META   "language": "python",
-- META   "language_group": "synapse_pyspark"
-- META }
