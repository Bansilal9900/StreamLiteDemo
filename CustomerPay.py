import streamlit as st

import pandas as pd

import snowflake.connector
import uuid
 

SNOWFLAKE_USER = 'bkeloth'

SNOWFLAKE_PASSWORD = 'Bansi@2022'

SNOWFLAKE_ACCOUNT = 'anblickspartner.east-us-2.azure'

SNOWFLAKE_DATABASE = 'DBT_PRACTICE_DB'

SNOWFLAKE_WAREHOUSE = 'POC_DATAHUB_GX_WH'

SNOWFLAKE_ROLE = 'SYSADMIN'
SNOWFLAKE_SCHEMA ='MAIN'

 

# Streamlit app

def main():

    st.title('Streamlit Snowflake Connection')

 

    # Create Snowflake connection

    snowflake_config = snowflake.connector.connect(

        user=SNOWFLAKE_USER,

        password=SNOWFLAKE_PASSWORD,

        account=SNOWFLAKE_ACCOUNT,

        warehouse=SNOWFLAKE_WAREHOUSE,

        database=SNOWFLAKE_DATABASE,

        role = SNOWFLAKE_ROLE,

        schema = SNOWFLAKE_SCHEMA

    )

 

 

 

    # query = "SELECT DISTINCT FIRST_NAME FROM MAIN.CUSTOMERS"

    # v_cur = conn.cursor()

    # results = v_cur.execute(query)

    # vendor_names = [result[0] for result in results]

 

    # option1 = st.selectbox('Choose Vendor Name', vendor_names)

    # st.write('You selected:', option1)

   

    # v_sql = f"SELECT First_name, Number_of_orders FROM MAIN.CUSTOMERS WHERE FIRST_NAME = '{option1}'"

    # query = v_sql

    # v_cur = conn.cursor()

    # results = v_cur.execute(query).fetchall()

 

    # df = pd.DataFrame(results, columns=["First_name", "Number_of_orders"])

    # edited_df = st.data_editor(df, on_change=lambda: None)

    # st.session_state.data_editor = edited_df

    # favorite_command = st.session_state.data_editor.loc[st.session_state.data_editor["First_name"] == "Sarah"]["Number_of_orders"].iloc[0]

    # #print(favorite_command)
    # print (option1)
 

    # #edited_df = st.data_editor(df, column_config={"First_name": "Streamlit_Command", "Number_of_orders": "Number of Orders"})

    # #st.data_editor(df, key="data_editor")

    # st.write("Here's the session state:",favorite_command)

    # st.write(st.session_state["data_editor"])

    # st.write("Saving values...")

    # edited_df.to_csv("data.csv")

 

    # Write the edited values to the database

# sql = f"UPDATE MAIN.CUSTOMERS SET Number_of_orders = '{favorite_command}' WHERE FIRST_NAME = '{option1}'"

# v_cur.execute(sql)

# conn.commit()

# st.write("Values saved!")

# conn.close()



# Generate a unique identifier




# Sample DataFrame

    data = {
        'Name': ['Alice', 'Bob', 'Charlie'],
        'Age': [25, 30, 22],
        'City': ['New York', 'Los Angeles', 'Chicago']
    }
    df = pd.DataFrame(data)
    editiable_df=st.data_editor(df)
    # Establish Snowflake connection
    connection = snowflake_config
    cursor = connection.cursor()
    unique_id = uuid.uuid4()
    # Define the target table name
    target_table = 'Your_Target_Table'

    # Create a new stage (optional, but recommended for data loading)
    stage_name = 'MAIN.EmpData'
    file_name ="GSC_"+str(unique_id)+'.csv'
    cursor.execute(f"CREATE OR REPLACE STAGE {stage_name}")

    # Convert DataFrame to CSV and stage it
    editiable_df.to_csv(file_name,index=False, header=False,sep=',', encoding='utf-8')
    st.write(editiable_df)
    cursor.execute(f"PUT file://{file_name} @EmpData")
    print(f"PUT file://{file_name} @EmpData")
    #cursor.execute(f"PUT file://data.csv @EmpData")
    #cursor.execute(f"COPY INTO MAIN.TEMP_TABLE FROM (SELECT $1,$2,$3 FROM {stage_location} FILE_FORMAT=(TYPE=CSV))")

    # Commit and close connection
    connection.commit()
    cursor.close()
    connection.close()

    print("Data has been inserted into the target table in Snowflake.")


if __name__ == '__main__':

    main()
