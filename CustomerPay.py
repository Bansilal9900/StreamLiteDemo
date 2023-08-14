"""
Creates a new datafame based on selected column from existing dataframe.
"""

import pandas as pd
import streamlit as st 



data = {
    'Name': ['Alice', 'Bob', 'Charlie'],
    'Age': [25, 30, 22],
    'City': ['New York', 'Los Angeles', 'Chicago']
}
df = pd.DataFrame(data)
editiable_df=st.data_editor(df,column_config(""))
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
csv_data = df.to_csv(file_name,index=False, header=False,sep=',', encoding='utf-8')
cursor.execute(f"PUT file://{file_name} @EmpData")
print(csv_data)
stage_location = f'@{stage_name}'
stage_uri = f'{stage_location}/data.csv'
#csv_file = io.StringIO(csv_data)
print("____________________________________________")
print(unique_id)
print(f"PUT file://data.csv @{stage_name}")
#cursor.execute(f"PUT file://data.csv @EmpData")
#cursor.execute(f"COPY INTO MAIN.TEMP_TABLE FROM (SELECT $1,$2,$3 FROM {stage_location} FILE_FORMAT=(TYPE=CSV))")

# Commit and close connection
connection.commit()
cursor.close()
connection.close()

print("Data has been inserted into the target table in Snowflake.")
