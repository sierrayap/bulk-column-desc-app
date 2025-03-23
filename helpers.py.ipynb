{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "5377eeca-dd0d-4b6a-ba18-879f5a17cd64",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "from pyspark.sql import SparkSession\n",
    "from pyspark.sql.functions import lit, col, regexp_replace\n",
    "from pyspark.sql.types import StructType, StructField, StringType\n",
    "import random\n",
    "\n",
    "# Initialize Spark session (ensure it's available)\n",
    "spark = SparkSession.builder.appName(\"StreamlitApp\").getOrCreate()\n",
    "\n",
    "# Function to get the list of tables in the schema\n",
    "def get_table_list(schema_location: str):\n",
    "    tables = spark.sql(f\"SHOW TABLES IN {schema_location}\").select(\"tableName\").collect()\n",
    "    return tables\n",
    "\n",
    "# Function to get schema info (columns, data types, comments) for a given table\n",
    "def get_schema_info(table_location: str):\n",
    "    df = spark.read.table(table_location)\n",
    "    schema = StructType([StructField(\"Column Name\", StringType(), True), StructField(\"Data Type\", StringType(), True), StructField(\"Comment\", StringType(), True)])\n",
    "    schema_info = spark.sql(f\"DESCRIBE TABLE {table_location}\").collect()\n",
    "    schema_df = spark.createDataFrame(schema_info, schema)\n",
    "    return schema_df\n",
    "\n",
    "# Loop through tables and get schema information for all columns\n",
    "def loop_schema_info(table_list: list):\n",
    "    schema = StructType([StructField(\"Table Name\", StringType(), True), StructField(\"Column Name\", StringType(), True), StructField(\"Data Type\", StringType(), True), StructField(\"Comment\", StringType(), True)])\n",
    "    df = spark.createDataFrame([], schema)\n",
    "    for table in table_list:\n",
    "        table_loc = f\"{table.tableName}\"  # Assuming tables are in the current catalog/schema\n",
    "        df_add = get_schema_info(table_loc).withColumn(\"Table Name\", lit(table.tableName))\n",
    "        df_add = df_add.select(\"Table Name\", *[col for col in df_add.columns if col != \"Table Name\"])\n",
    "        df = df.union(df_add)\n",
    "    return df\n",
    "\n",
    "# Function to generate AI-based column descriptions\n",
    "def generate_column_desc(table_name, column_name, catalog, schema):\n",
    "    # Example query to generate column descriptions using an AI model\n",
    "    query = f\"\"\"\n",
    "    SELECT ai_query(\n",
    "        'databricks-meta-llama-3-3-70b-instruct', \n",
    "        'Generate a 1 sentence description of the type of information that the column ''{column_name}'' from the table ''{table_name}'' in schema {schema} would contain.'\n",
    "    ) AS column_description \n",
    "    \"\"\"\n",
    "    column_desc = spark.sql(query)\n",
    "    column_desc = column_desc.withColumn(\"cleaned_column\", regexp_replace(\"column_description\", \"'\", \"\"))\n",
    "    column_desc_list = column_desc.collect()\n",
    "    if column_desc_list:\n",
    "        return column_desc_list[0]['cleaned_column']\n",
    "    else:\n",
    "        return \"Description not available\"\n",
    "\n",
    "# Function to apply AI descriptions to all columns in a table\n",
    "def populate_column_desc(df, catalog, schema):\n",
    "    column_descriptions = []\n",
    "    for row in df.collect():\n",
    "        table_name = row['Table Name']\n",
    "        column_name = row['Column Name']\n",
    "        comment = generate_column_desc(table_name, column_name, catalog, schema)\n",
    "        column_descriptions.append((table_name, column_name, comment))\n",
    "    new_columns = ['Table Name', 'Column Name', 'Comment']\n",
    "    return spark.createDataFrame(column_descriptions, new_columns)\n",
    "\n",
    "# Function to import updated column descriptions into the database\n",
    "def import_column_desc(df, catalog, schema):\n",
    "    for row in df.collect():\n",
    "        table_name = row[\"Table Name\"]\n",
    "        column_name = row[\"Column Name\"]\n",
    "        comment = row[\"Comment\"]\n",
    "        query = f\"COMMENT ON COLUMN {catalog}.{schema}.{table_name}.{column_name} IS '{comment}';\"\n",
    "        try:\n",
    "            spark.sql(query)\n",
    "        except Exception as e:\n",
    "            print(f\"Error executing query: {query}\")\n",
    "            print(f\"Error: {e}\")"
   ]
  }
 ],
 "metadata": {
  "application/vnd.databricks.v1+notebook": {
   "computePreferences": null,
   "dashboards": [],
   "environmentMetadata": {
    "base_environment": "",
    "environment_version": "2"
   },
   "language": "python",
   "notebookMetadata": {
    "pythonIndentUnit": 2
   },
   "notebookName": "helpers.py",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
