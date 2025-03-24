from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType
import random

# Remove Spark session creation - use the one from app.py instead

# Function to get the list of tables in the schema
def get_table_list(schema_location: str, spark: SparkSession):
    tables = spark.sql(f"SHOW TABLES IN {schema_location}").select("tableName").collect()
    return tables

# Function to get schema info (columns, data types, comments) for a given table
def get_schema_info(table_location: str, spark: SparkSession):
    df = spark.read.table(table_location)
    schema = StructType([StructField("Column Name", StringType(), True), StructField("Data Type", StringType(), True), StructField("Comment", StringType(), True)])
    schema_info = spark.sql(f"DESCRIBE TABLE {table_location}").collect()
    schema_df = spark.createDataFrame(schema_info, schema)
    return schema_df

# Loop through tables and get schema information for all columns
def loop_schema_info(table_list: list, spark: SparkSession):
    schema = StructType([StructField("Table Name", StringType(), True), StructField("Column Name", StringType(), True), StructField("Data Type", StringType(), True), StructField("Comment", StringType(), True)])
    df = spark.createDataFrame([], schema)
    for table in table_list:
        table_loc = f"{table.tableName}"  # Assuming tables are in the current catalog/schema
        df_add = get_schema_info(table_loc, spark).withColumn("Table Name", lit(table.tableName))
        df_add = df_add.select("Table Name", *[col for col in df_add.columns if col != "Table Name"])
        df = df.union(df_add)
    return df

# Function to generate AI-based column descriptions
def generate_column_desc(table_name, column_name, catalog, schema, spark: SparkSession):
    # Example query to generate column descriptions using an AI model
    query = f"""
    SELECT ai_query(
        'databricks-meta-llama-3-3-70b-instruct', 
        'Generate a 1 sentence description of the type of information that the column ''{column_name}'' from the table ''{table_name}'' in schema {schema} would contain.'
    ) AS column_description 
    """
    column_desc = spark.sql(query)
    column_desc = column_desc.withColumn("cleaned_column", regexp_replace("column_description", "'", ""))
    column_desc_list = column_desc.collect()
    if column_desc_list:
        return column_desc_list[0]['cleaned_column']
    else:
        return "Description not available"

# Function to apply AI descriptions to all columns in a table
def populate_column_desc(df, catalog, schema, spark: SparkSession):
    column_descriptions = []
    for row in df.collect():
        table_name = row['Table Name']
        column_name = row['Column Name']
        comment = generate_column_desc(table_name, column_name, catalog, schema, spark)
        column_descriptions.append((table_name, column_name, comment))
    new_columns = ['Table Name', 'Column Name', 'Comment']
    return spark.createDataFrame(column_descriptions, new_columns)

# Function to import updated column descriptions into the database
def import_column_desc(df, catalog, schema, spark: SparkSession):
    for row in df.collect():
        table_name = row["Table Name"]
        column_name = row["Column Name"]
        comment = row["Comment"]
        query = f"COMMENT ON COLUMN {catalog}.{schema}.{table_name}.{column_name} IS '{comment}';"
        try:
            spark.sql(query)
        except Exception as e:
            print(f"Error executing query: {query}")
            print(f"Error: {e}")
