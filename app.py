{
 "cells": [
  {
   "cell_type": "code",
   "execution_count": 0,
   "metadata": {
    "application/vnd.databricks.v1+cell": {
     "cellMetadata": {},
     "inputWidgets": {},
     "nuid": "509acc87-71fb-4b6f-802c-505d283c2ed9",
     "showTitle": false,
     "tableResultSettingsMap": {},
     "title": ""
    }
   },
   "outputs": [],
   "source": [
    "import streamlit as st\n",
    "import pandas as pd\n",
    "from helpers import get_table_list, loop_schema_info, populate_column_desc, import_column_desc\n",
    "\n",
    "# Streamlit UI\n",
    "def main():\n",
    "    st.title(\"Bulk Column Description Editor\")\n",
    "\n",
    "    # Step 1: Input fields for catalog and schema\n",
    "    catalog = st.text_input(\"Catalog\", \"\")\n",
    "    schema = st.text_input(\"Schema\", \"\")\n",
    "\n",
    "    if catalog and schema:\n",
    "        # Step 2: Fetch tables list based on catalog.schema\n",
    "        schema_location = f\"{catalog}.{schema}\"\n",
    "        table_list = get_table_list(schema_location)\n",
    "        \n",
    "        if table_list:\n",
    "            st.write(f\"Found {len(table_list)} tables in {catalog}.{schema}.\")\n",
    "\n",
    "            # Step 3: Get schema info for each table\n",
    "            df = loop_schema_info(table_list)\n",
    "            st.write(f\"Columns descriptions for the tables in {catalog}.{schema}:\")\n",
    "            \n",
    "            # Step 4: Display the editable table for column descriptions\n",
    "            edited_df = st.experimental_data_editor(df)\n",
    "            \n",
    "            # Step 5: Accept and import button\n",
    "            if st.button('Accept and Import'):\n",
    "                if edited_df is not None:\n",
    "                    import_column_desc(edited_df, catalog, schema)\n",
    "                    st.success(\"Column descriptions updated successfully.\")\n",
    "                else:\n",
    "                    st.error(\"No data to import. Please ensure changes are made to the table.\")\n",
    "\n",
    "        else:\n",
    "            st.warning(f\"No tables found in {catalog}.{schema}.\")\n",
    "    else:\n",
    "        st.warning(\"Please provide both Catalog and Schema.\")\n",
    "\n",
    "# Run the Streamlit app\n",
    "if __name__ == \"__main__\":\n",
    "    main()"
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
   "notebookName": "app.py",
   "widgets": {}
  },
  "language_info": {
   "name": "python"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 0
}
