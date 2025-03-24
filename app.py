from pyspark.sql import SparkSession
import streamlit as st
import pandas as pd
from helpers import get_table_list, loop_schema_info, populate_column_desc, import_column_desc

# Create Spark session directly in app.py
spark = SparkSession.builder \
    .appName("Bulk Column Description Editor") \
    .getOrCreate()


# Streamlit UI
def main():
    st.title("Bulk Column Description Editor")

    # Step 1: Input fields for catalog and schema
    catalog = st.text_input("Catalog", "")
    schema = st.text_input("Schema", "")

    if catalog and schema:
        # Step 2: Fetch tables list based on catalog.schema
        schema_location = f"{catalog}.{schema}"
        table_list = get_table_list(schema_location)
        
        if table_list:
            st.write(f"Found {len(table_list)} tables in {catalog}.{schema}.")

            # Step 3: Get schema info for each table
            df = loop_schema_info(table_list)
            st.write(f"Columns descriptions for the tables in {catalog}.{schema}:")
            
            # Step 4: Display the editable table for column descriptions
            edited_df = st.experimental_data_editor(df)
            
            # Step 5: Accept and import button
            if st.button('Accept and Import'):
                if edited_df is not None:
                    import_column_desc(edited_df, catalog, schema)
                    st.success("Column descriptions updated successfully.")
                else:
                    st.error("No data to import. Please ensure changes are made to the table.")

        else:
            st.warning(f"No tables found in {catalog}.{schema}.")
    else:
        st.warning("Please provide both Catalog and Schema.")

# Run the Streamlit app
if __name__ == "__main__":
    main()