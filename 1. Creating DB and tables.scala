// Databricks notebook source
/*
  1. Create the DB and use it.
  2. Function defined to create the tables and ingest data.
  3. Calling function with a loop 
*/

// COMMAND ----------

// MAGIC %sql
// MAGIC DROP DATABASE IF EXISTS original_tables;
// MAGIC 
// MAGIC --Creating database to write the original tables before processing
// MAGIC CREATE DATABASE IF NOT EXISTS original_tables;
// MAGIC USE original_tables;

// COMMAND ----------

//Load and create the table reading the raw data
def loadAndCreate (nameWeb: String) : Unit = {
  
    print("NameWeb:" + nameWeb)
    
    // File location and type
    var file_location = "/FileStore/tables/" + nameWeb.capitalize +"/*_" + nameWeb + ".json"
    var file_type = "json"
  
    // Loading JSON files 
    val df = spark.read.format(file_type) 
      .option("inferSchema", "false")  //Not infer schema
      .option("header", "false") //Not header
      .option("sep", ",") 
      .load(file_location)
      .drop("_corrupt_record") //Drop corrupt record column

    //Creating temp View 
    var temp_table_name = "original_" + nameWeb + "_" + file_type 
    df.createOrReplaceTempView(temp_table_name)
    var permanent_table_name = "original_"  + nameWeb
  
    //Saving dataframe in Parquet Format as a table 
    df.write.format("parquet").saveAsTable(permanent_table_name)
  
}


// COMMAND ----------

//Webs contain info of json files 
val webs: List[String] = List("wipoid", "appinformatica", "coolmod")

//Loop to call the function
webs.foreach(x => loadAndCreate(x))


// COMMAND ----------

val webs: List[String] = List("wipoid", "appinformatica", "coolmod")

webs.foreach(x => loadAndCreate(x))


// COMMAND ----------

//File location and type
val file_location = "/FileStore/tables/AppInformatica/*_appinformatica.json"
val file_type = "json"

//JSON options
val infer_schema = "true"


// The applied options are for JSON files. For other file types, these will be ignored.
val df = spark.read.format(file_type) 
  .option("inferSchema", infer_schema) 
  .load(file_location)

val df2 = df.drop("_corrupt_record") //First signature

//Create a view or table
temp_table_name = "original_appinformatica_json"
df.createOrReplaceTempView(temp_table_name)
permanent_table_name = "original_appinformatica"

df.write.format("parquet").saveAsTable(permanent_table_name)