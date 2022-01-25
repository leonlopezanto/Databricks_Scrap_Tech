// Databricks notebook source
/*
  On this notebook we can make operations about de File System.
  
  Usually, we create and delete the folders containing the json data uploaded.
  
*/

// COMMAND ----------

import org.apache.hadoop.fs.{Path, FileSystem}
dbutils.fs.help()

// COMMAND ----------

//Delete files from a folder
dbutils.fs.rm("/FileStore/tables/AppInformatica", false)

// COMMAND ----------

//Creating directories to raw data

//dbutils.fs.mkdirs("/FileStore/tables/Appinformatica")
//dbutils.fs.mkdirs("/FileStore/tables/Wipoid")
//dbutils.fs.mkdirs("/FileStore/tables/Coolmod")

// COMMAND ----------

