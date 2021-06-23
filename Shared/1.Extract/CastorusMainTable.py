# Databricks notebook source
# DBTITLE 1,Installation package
import pandas as pd
import requests
from selenium import webdriver
from bs4 import BeautifulSoup
pd.options.display.max_colwidth=1000
from urllib.request import urlopen
from urllib.error import HTTPError
import re
from datetime import datetime
import urllib.request
import time
import lxml
import bs4.builder._lxml
import lxml.etree

# COMMAND ----------

# DBTITLE 1,Function definition
#Get full table from url
def CastorusMainTable(url):
  ids=[]
  redirects=[]
  #read table with the id myTableResult
  Maintable=pd.read_html(url, attrs = {'id': 'myTableResult'})
  #convert list to dataframe
  df_table=pd.concat(Maintable)
  #Get URL HTML source page
  response = requests.get(url)
  #setup BS4 function
  soup = BeautifulSoup(response.text, 'html.parser')
  #Fetch table in HTML
  table = soup.find('table')
  links = []
  # For each row, extract URL link to apartment info
  for tr in table.findAll("tr"):
      trs = tr.findAll("td")
      for each in trs:
          try:
            #We extract the URL from A HREF
            link = each.find('a')['href']
            #We reconstruct the full URL
            links.append("https://www.castorus.com"+link)
            #Regex to extract the aprtment ID
            id=re.search("(\w\d+)", link).group(0)
            ids.append(id)
            #We reconstruct the redirection link from the id and the function Source Link
            redirect=getSourceLink("https://www.castorus.com/r.php?redirect="+id)
            redirects.append(redirect)
          except:
            pass
  df_table["id"]=ids
  df_table["SourceLink"]=redirects
  df_table['Link'] = links
  df_table['UpdateDateMainTable'] = datetime.now()
  return df_table

#Get Source link of the apt
def getSourceLink(url):
  try:
    #get URL HTML 
    html = urlopen(url)
  except HTTPError as e:
      return None
  try:
    #Set up BS4 function
    bsObj = BeautifulSoup(html.read(), "lxml")
    link = bsObj.body.script
    if link == None:
        print("SourceLink could not be found")
    else:
      #Convert link to dataframe
      SourceLinkRaw=pd.DataFrame(link).convert_dtypes().to_string()
      #REGEX to extract url SOurce Link
      SourceLink = re.search("(https?:\/\/[^\"]*)", SourceLinkRaw).group(0)       
  except AttributeError as e:
      return None
  return SourceLink

# COMMAND ----------

# DBTITLE 1,Main
url='https://www.castorus.com/s/Levallois-perret,38504,-------------------------'
#Convert Pandas DF to SPark DF, get Castorus Main table and rename column
sp_MainCastorus=spark.createDataFrame(CastorusMainTable(url).rename(columns = {'vue le':'vue'}, inplace = False))
#Create SQL view 
sp_MainCastorus.distinct().createOrReplaceTempView('main_castorus_updates')

# COMMAND ----------

# DBTITLE 1,Upsert
# MAGIC %sql
# MAGIC MERGE INTO bronze.main_castorus
# MAGIC USING main_castorus_updates
# MAGIC ON bronze.main_castorus.id=main_castorus_updates.id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

#Return a evalue when using dbutils.notebook.run for orchestration
dbutils.notebook.exit('Success')

# COMMAND ----------

# DBTITLE 1,Overwrite file & Create delta table
#sp_MainCastorus.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/bronze/main_castorus") 
#spark.sql("CREATE TABLE IF NOT EXISTS bronze.main_castorus USING DELTA LOCATION '/FileStore/bronze/main_castorus'")
