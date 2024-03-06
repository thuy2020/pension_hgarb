import html
import time
import os
from io import StringIO
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import shutil
from sqlalchemy import text

print(time.ctime() + ": Converting ppd Excel file to a data frame")
ppd = pd.read_excel("PensionInvestmentPerformanceDetailed.xlsx")

print(time.ctime() + ": Converting ppd column names to lower case")
ppd.columns = map(str.lower, ppd.columns)

# Get Column List
print(time.ctime() + ": Getting ppd column names")
ppd_cols = list(ppd)
ppd_cols.remove('ppd_id')
ppd_cols.remove('fy')

pd.options.display.max_columns = 500  # PPD has more than 255 columns

print(time.ctime() + ": Getting plan and attribute ids from the database")
engine = create_engine("postgresql://u9a2ef5lju8rrc:p54e096881444359c1c6cf3992281aaeecbe8f2bd5aa5e993f4811c2e7316cfb2@ec2-3-209-200-73.compute-1.amazonaws.com:5432/d629vjn37pbl3l")
plans = pd.read_sql_query("select ppd_id, id as plan_id from plan where ppd_id is not null", con=engine)
attribs = pd.read_sql_query("select id as attribute_id, lower(line_item_code) as line_item_lower from plan_attribute where data_source_id = 14", con=engine)
attribs.to_excel('attribs.xlsx', index=None)

print(time.ctime() + ": Merging plan_ids into the dataframe")
ppd_mapped = pd.merge(ppd, plans, on="ppd_id", how="left")
print(time.ctime() + ": Unpivoting the dataframe")
ppd_melted = ppd_mapped.melt(id_vars=['plan_id','fy'],value_vars=ppd_cols)
ppd_melted.drop(ppd_melted[ppd_melted['variable'].str.contains("bnchmrk")].index, inplace = True)
ppd_melted.drop(ppd_melted[ppd_melted['variable'].str.match("eegroupid")].index, inplace = True)
ppd_melted.drop(ppd_melted[ppd_melted['variable'].str.match("planname")].index, inplace = True)
ppd_melted.drop(ppd_melted[ppd_melted['variable'].str.match("ppd_id")].index, inplace = True)
ppd_melted.drop(ppd_melted[ppd_melted['variable'].str.match("tierid")].index, inplace = True)

ppd_melted.drop(ppd_melted[ppd_melted['variable'].str.match("DoubleCounting_")].index, inplace = True)
ppd_melted.drop(ppd_melted[ppd_melted['variable'].str.match("test_")].index, inplace = True)
ppd_melted.to_csv("ppd-inv-melted.csv", index=None)

print(time.ctime() + ": Merging attribute_ids into unpivoted data frame")
ppd_import = pd.merge(ppd_melted, attribs, left_on='variable', right_on='line_item_lower', how='left')
ppd_import.rename(columns = {'fy': 'year', 'value': 'attribute_value'}, inplace=True)
ppd_import.dropna(subset=['attribute_value'])
ppd_import.dropna(subset=['year'])
ppd_import.drop(ppd_import[ppd_import['attribute_value'] == 0].index, inplace = True)
ppd_import['attribute_value'] = ppd_import.attribute_value.astype('str')
ppd_import.attribute_value = ppd_import.attribute_value.str.slice(start=0, stop=255)
ppd_import["year"] = ppd_import["year"].astype('str')
ppd_import.year = ppd_import.year.str.slice(start=0, stop=4)
ppd_import["attribute_id"] = ppd_import["attribute_id"].astype('str')
ppd_import.attribute_id = ppd_import.attribute_id.str.slice(start=0, stop=3)
ppd_import.drop(ppd_import[ppd_import['attribute_value'].str.match("nan")].index, inplace = True)

print(time.ctime() + ": Clearing previously loaded data on Postgres")
connection = engine.connect()
truncate_statement = text("TRUNCATE TABLE new_ppd_import")
# result = connection.execute("truncate table new_ppd_import")
result = connection.execute(truncate_statement)
connection.close()

print(time.ctime() + ": Write to file like object")
sio = StringIO()
sio.write(ppd_import.to_csv(columns=['plan_id','year','attribute_id','attribute_value'], index=None))
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
    c.copy_expert("copy public.new_ppd_import (plan_id, year, plan_attribute_id, attribute_value) FROM STDIN DELIMITER ',' CSV HEADER QUOTE '\"' ESCAPE '''';", sio)
    conn.commit()

print(time.ctime() + ": Loading rows into plan_annual_attribute table")
connection = engine.connect()
delete_statement = text("DELETE FROM new_ppd_import WHERE year = 'nan' OR attribute_value = 'nan'")
result = connection.execute(delete_statement)
# result = connection.execute("delete from new_ppd_import where year='nan' or attribute_value = 'nan';")
cursor = conn.cursor()
cursor.callproc("load_ppd_investment_data")
conn.commit()
cursor.close()

# Update Master Attribute Table
print(time.ctime() + ": Updating master attribute table")
cursor = conn.cursor()
cursor.callproc("update_plan_annual_master_attribute")
conn.commit()
cursor.close()
conn.close()
connection.close()
print(time.ctime() + ": Run complete")