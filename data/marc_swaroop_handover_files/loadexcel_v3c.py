# Uploads Data from Reason Excel Template to Postgres Database
# Revised December 5, 2021 to Only Update Master Tables for the Plan(s) Being Updated

import time
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from io import StringIO
import shutil
import os
from sqlalchemy import text


print(time.ctime() + ": Connecting to database")

engine = create_engine("postgresql://u9a2ef5lju8rrc:p54e096881444359c1c6cf3992281aaeecbe8f2bd5aa5e993f4811c2e7316cfb2@ec2-3-209-200-73.compute-1.amazonaws.com:5432/d629vjn37pbl3l")
connection = engine.connect()

# conn = psycopg2.connect(dbname="d629vjn37pbl3l", user="u9a2ef5lju8rrc", password="p54e096881444359c1c6cf3992281aaeecbe8f2bd5aa5e993f4811c2e7316cfb2",
# 						host="ec2-3-209-200-73.compute-1.amazonaws.com")

# , options=f"-c statement_timeout=10000"

"C://Users//swaro//reason Dropbox//Swaroop Bhagavatula//Asana Dropbox//Pension_Reform//Research//Data_Analytics//NonShared_PensionSheet//HGarb_Updates_2020//Latest"

# folder_path = "/Users/anhtu/reason Dropbox/steve vu/Asana Dropbox/Pension_Reform/Research/Data_Analytics/NonShared_PensionSheet/HGarb_Updates_2020/Latest"
file = "Arkansas_JRS_updated_2021.xlsx"
# for file in os.listdir("C://Users//steve vu//Dropbox (reason)//Asana Dropbox//Pension_Reform//Research//Data_Analytics//NonShared_PensionSheet//HGarb_Updates_2020//Latest"):
# for file in os.listdir(folder_path):
	# if file.endswith(".xlsx"):

# df = pd.read_excel(os.path.join("C://Users//swaro//Dropbox (reason)//Asana Dropbox//Pension_Reform//Research//Data_Analytics//NonShared_PensionSheet//HGarb_Updates_2020//Latest",file), sheet_name="Database Input", usecols="E:H", skiprows=2)
# df = pd.read_excel(os.path.join(folder_path, file), sheet_name="Database Input", usecols="E:H", skiprows=2)
df = pd.read_excel("Arkansas_JRS_updated_2021.xlsx", sheet_name="Database Input", usecols="E:H", skiprows=2)
df.drop_duplicates(subset=['plan_id', 'year', 'plan_attribute_id'], inplace=True)

#Get Plan ID
plan_id = str(df['plan_id'][0].item())

truncate_statement = text("TRUNCATE TABLE new_excel_import")
result = connection.execute(truncate_statement)

print(time.ctime() + ": Write to file like object")
sio = StringIO()
sio.write(df.to_csv(columns=['plan_id','year','plan_attribute_id','attribute_value'], index=None))
# with open ('sio.csv', 'w', encoding='utf8') as fd:
with open ('sio.csv', 'w') as fd:
	sio.seek (0)
	shutil.copyfileobj (sio, fd)

print(time.ctime() + ": Uploading dataframe to Postgres")
sio.seek(0)
conn = psycopg2.connect(
			dbname="d629vjn37pbl3l", 
			user="u9a2ef5lju8rrc", 
			password="p54e096881444359c1c6cf3992281aaeecbe8f2bd5aa5e993f4811c2e7316cfb2",
			host="ec2-3-209-200-73.compute-1.amazonaws.com")

with conn.cursor() as c:
	c.copy_expert("copy public.new_excel_import (plan_id, year, plan_attribute_id, attribute_value) FROM STDIN DELIMITER ',' CSV HEADER QUOTE '\"' ESCAPE '''';", sio)
	conn.commit()

cursor = conn.cursor()
cursor.callproc("load_excel_template_data")
conn.commit()
cursor.callproc("fill_zero_allocations", (plan_id,'2021'))  # Fill all blank 2021 allocation data with zeroes to fully override PPD
conn.commit()
cursor.callproc("update_one_plan_annual_master_attribute", (plan_id,))
conn.commit()
cursor.close()

print(time.ctime() + ": Loaded " + file)

# Update Views
print(time.ctime() + ": Updating views")
cursor = conn.cursor()
cursor.callproc("refresh_views")
conn.commit()
cursor.close()
conn.close()
connection.close()
print(time.ctime() + ": Finished")