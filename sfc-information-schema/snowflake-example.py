# Import required libraries
import json
# Snowpark
from venv import create
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import IntegerType
from snowflake.snowpark.functions import avg, sum, col, call_udf, lit, call_builtin, year
# Pandas
import pandas as pd
#Streamlit
import streamlit as st

#Set page context
st.set_page_config(
    page_title="Snowflake Information Schema Example",
    page_icon="🧊",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://developers.snowflake.com',
        'About': "This is an *extremely* cool app powered by Snowpark for Python, Streamlit, and Snowflake Marketplace"
    }
)

def create_session():
    with open('creds.json') as f:
        cp = json.load(f)

    conn = Session.builder.configs(cp).create()

    return conn



session = create_session()
st.text(session.sql('select current_warehouse(), current_database(), current_schema()').collect())

databases_df = session.table("DATABASES")
tables_df = session.table("TABLES")
tables_per_schema_df = session.sql("select distinct table_schema, count(table_name) NumTables from snowflake.information_schema.tables group by table_schema")

col1, col2, col3 = st.columns(3)
with st.container():
    with col1:
            st.header("Databases")
            st.dataframe(databases_df.toPandas())
    with col2:
        st.header("Tables")
        st.dataframe(tables_df.toPandas())
    with col3:
        st.header("Tables Per Schema")
        st.dataframe(tables_per_schema_df.toPandas())

st.bar_chart(data=tables_df.toPandas()['TABLE_SCHEMA'].value_counts())
