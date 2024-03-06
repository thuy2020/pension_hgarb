# Load Latest PPD Update into Reason Pension Database

import html
import time
#from selenium import webdriver
#from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
#from selenium.webdriver.common.keys import Keys
#from selenium.common.exceptions import TimeoutException
#from selenium.webdriver.support.ui import WebDriverWait
#from selenium.webdriver.support.ui import Select
import os
from io import StringIO
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import shutil
from sqlalchemy import text



# Removing code that automatically downloads the file - 10/12/2022
# As result, the latest file named ppd-data-latest.csv must be in the current folder before running this script

#if os.path.exists("ppd-data-latest.csv"):
#  os.remove("ppd-data-latest.csv")

#if os.path.exists("ppd-out.xlsx"):
#  os.remove("ppd-out.xlsx")

#if os.path.exists("ppd-import.xlsx"):
#  os.remove("ppd-import.xlsx")

#if os.path.exists("attribs.xlsx"):
#  os.remove("attribs.xlsx")

#if os.path.exists("ppd-melted.xlsx"):
#  os.remove("ppd-melted.xlsx")

#options = webdriver.ChromeOptions() 
#options.add_argument("--start-maximized")
#prefs = {
#        'download.default_directory': "D:\\PPD",
#        'download.prompt_for_download': False,
#        'download.directory_upgrade': True,
#        'plugins.always_open_pdf_externally': True,
#    }
#options.add_experimental_option("prefs",prefs)

#capabilities = DesiredCapabilities.CHROME
#capabilities["marionette"] = True

#path_to_chromedriver = "C:\\windows\\system32\\chromedriver.exe"
#driver = webdriver.Chrome(executable_path= path_to_chromedriver, chrome_options=options)
#print(time.ctime() + ": Downloading ppd file")

#driver.get("https://publicplansdata.org/public-plans-database/download-full-data-set/")

#time.sleep(0.5)
#driver.find_element_by_link_text("Download Full Data Set").click()
#print(time.ctime() + ": Sleeping 15 Seconds")
#time.sleep(15.0)
#print(time.ctime() + ": Closing Driver")
#driver.close()

print(time.ctime() + ": Converting ppd csv file to a data frame")
# ppd = pd.read_csv("ppd-data-latest.csv", encoding="ANSI", low_memory=False)
ppd = pd.read_csv("ppd-data-latest.csv", encoding="ISO-8859-1", low_memory=False)

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
attribs = pd.read_sql_query("select id as attribute_id, lower(attribute_column_name) as attribute_column_name_lcase from plan_attribute where data_source_id = 2", con=engine)
attribs.to_excel('attribs.xlsx', index=None)

print(time.ctime() + ": Clearing previously loaded data on Postgres")
connection = engine.connect()
truncate_statement = text("TRUNCATE TABLE new_ppd_import")
result = connection.execute(truncate_statement)
# result = connection.execute("truncate table new_ppd_import")
connection.close()

print(time.ctime() + ": Merging plan_ids into the dataframe")
ppd_mapped = pd.merge(ppd, plans, on="ppd_id", how="left")
ppd_mapped.to_excel("ppd-out.xlsx", index=None)
print(time.ctime() + ": Unpivoting the dataframe")
ppd_melted = ppd_mapped.melt(id_vars=['plan_id','fy'],value_vars=ppd_cols)
ppd_melted.to_csv("ppd-melted.csv", index=None)
#ppd_melted['fy'] = ppd_melted.fy.astype('str')
#ppd_melted['value'] = ppd_melted.value.astype('str')

print(time.ctime() + ": Merging attribute_ids into unpivoted data frame")
ppd_import = pd.merge(ppd_melted, attribs, left_on='variable', right_on='attribute_column_name_lcase', how='left')
ppd_import.to_csv("ppd-import-0.csv", index=None)
ppd_import.rename(columns = {'fy': 'year', 'value': 'attribute_value'}, inplace=True)
ppd_import.to_csv("ppd-import-1.csv", index=None)
ppd_import.dropna(inplace=True)
# ppd_import = ppd_import.dropna(subset=['attribute_value'])
# ppd_import = ppd_import.dropna(subset=['year'])
# ppd_import = ppd_import.dropna(subset=['attribute_id'])
ppd_import.to_csv("ppd-import-2.csv", index=None)
ppd_import['attribute_value'] = ppd_import.attribute_value.astype('str')
ppd_import.attribute_value = ppd_import.attribute_value.str.slice(start=0, stop=255)
ppd_import["year"] = ppd_import["year"].astype('str')
ppd_import.year = ppd_import.year.str.slice(start=0, stop=4)
ppd_import["attribute_id"] = ppd_import["attribute_id"].astype('str')
# ppd_import["plan_attribute_id"] = ppd_import["attribute_id"].astype('str')
ppd_import.attribute_id = ppd_import.attribute_id.str.slice(start=0, stop=3)
# ppd_import.plan_attribute_id = ppd_import.plan_attribute_id.str.slice(start=0, stop=3)
ppd_import.to_csv("ppd-import-3.csv", index=None)

print(time.ctime() + ": Write to file like object")
sio = StringIO()
sio.write(ppd_import.to_csv(columns=['plan_id','year','attribute_id','attribute_value'], index=None))
# sio.write(ppd_import.to_csv(columns=['plan_id','year','plan_attribute_id','attribute_value'], index=None))

with open ('sio.csv', 'w') as fd:
  sio.seek(0)
  shutil.copyfileobj (sio, fd)

print(time.ctime() + ": Uploading dataframe to Postgres")
sio.seek(0)
conn = psycopg2.connect(dbname="d629vjn37pbl3l", user="u9a2ef5lju8rrc", password="p54e096881444359c1c6cf3992281aaeecbe8f2bd5aa5e993f4811c2e7316cfb2",host="ec2-3-209-200-73.compute-1.amazonaws.com")
with conn.cursor() as c:
    c.copy_expert("copy public.new_ppd_import (plan_id, year, plan_attribute_id, attribute_value) FROM STDIN DELIMITER ',' CSV HEADER QUOTE '\"' ESCAPE '''';", sio)
    # c.copy_expert("COPY public.new_ppd_import (plan_id, year, plan_attribute_id, attribute_value) FROM STDIN DELIMITER ',' CSV HEADER QUOTE '\"' ESCAPE E'\\\\';", sio)
    conn.commit()

print(time.ctime() + ": Loading rows into plan_annual_attribute table")
connection = engine.connect()
delete_statement = text("DELETE FROM new_ppd_import WHERE year = 'nan' OR attribute_value = 'nan'")
result = connection.execute(delete_statement)
# result = connection.execute("delete from new_ppd_import where year='nan' or attribute_value = 'nan';")
cursor = conn.cursor()
cursor.callproc("load_ppd_data")
conn.commit()
cursor.close()

# Load New Rows into plan_annual_attribute table
# result = connection.execute("insert into plan_annual_attribute (plan_id, year, plan_attribute_id, attribute_value) " + \
# "select distinct new_ppd_import.plan_id, new_ppd_import.year, new_ppd_import.plan_attribute_id, new_ppd_import.attribute_value " + \
# "from new_ppd_import " + \
# "left join plan_annual_attribute " + \
# "on new_ppd_import.plan_id = plan_annual_attribute.plan_id " + \
# "and new_ppd_import.year = plan_annual_attribute.year " + \
# "and new_ppd_import.plan_attribute_id = plan_annual_attribute.plan_attribute_id " + \
# "where plan_annual_attribute.plan_id is null;")

# Update previously uploaded rows in plan_annual_attribute table
# print(time.ctime() + ": Updating existing rows in plan_annual_attribute table")
# connection.execute("update plan_annual_attribute " + \
# "set attribute_value = new_ppd_import.attribute_value " + \
# "from new_ppd_import " + \
# "where plan_annual_attribute.plan_id = new_ppd_import.plan_id " + \
# "and plan_annual_attribute.year = new_ppd_import.year " + \
# "and plan_annual_attribute.plan_attribute_id = new_ppd_import.plan_attribute_id;")

# Update Master Attribute Table
print(time.ctime() + ": Updating master attribute table")
cursor = conn.cursor()
cursor.callproc("update_plan_annual_master_attribute")
conn.commit()
cursor.close()
conn.close()
connection.close()
print(time.ctime() + ": Run complete")
