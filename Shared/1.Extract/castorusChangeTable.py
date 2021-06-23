# Databricks notebook source
import pandas as pd
import requests
from selenium import webdriver
from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.error import HTTPError
import re
from datetime import datetime
import urllib.request

# COMMAND ----------

#Get all change of the apt
def getChange(Table):
  i=0
  ChangeTable=pd.DataFrame()
  for each in Table.iloc[:,0]:
    try:    
      i= i +1
      #read URL and get html
      html_change=pd.read_html(each, attrs = {'width': '98%'})
      #convert from list to DF
      df_change=pd.concat(html_change)
      #Regex to get ID
      id=re.search("(\w\d+)", each).group(0)
      df_change['id']=id
      df_change['UpdateDateChange'] = datetime.now()
      ChangeTable=ChangeTable.append(df_change)
    except:
      pass
  ChangeTable.rename(columns={0: 'Date_MDDYYYY', 1:'changeDescription'}, inplace=True)
  print(str(i) + ' on ' + str(len(castorus)))
  return ChangeTable


# COMMAND ----------

castorus=spark.sql('''SELECT Link
FROM silver.main_table mt
LEFT JOIN silver.meta_table md ON md.id=mt.id
where (UpdateDateChange is null OR UpdateDateChange < DATE_ADD(NOW(),-7)) ''').limit(100).toPandas()
sp_Castorus = spark.createDataFrame(getChange(castorus)).distinct().createOrReplaceTempView('castorus_change_updates')

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.castorus_change
# MAGIC USING castorus_change_updates
# MAGIC ON bronze.castorus_change.id=castorus_change_updates.id
# MAGIC and bronze.castorus_change.Date_MDDYYYY = castorus_change_updates.Date_MDDYYYY
# MAGIC and bronze.castorus_change.changeDescription =castorus_change_updates.changeDescription
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

#Return a evalue when using dbutils.notebook.run for orchestration
dbutils.notebook.exit('Success')

# COMMAND ----------

#sp_Castorus.distinct().write.mode("overwrite").option("overwriteschema", "true").format("delta").save("/FileStore/bronze/castorus_change") 
#spark.sql("CREATE TABLE IF NOT EXISTS bronze.castorus_change USING DELTA LOCATION '/FileStore/bronze/castorus_change'")
