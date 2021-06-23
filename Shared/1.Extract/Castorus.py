# Databricks notebook source
#Enviroment
#!apt-get update 
#!apt install chromium-chromedriver 
#pip install lxml then clear state 
# try https://stackoverflow.com/questions/17766725/how-to-re-install-lxml
#or try https://stackoverflow.com/questions/24398302/bs4-featurenotfound-couldnt-find-a-tree-builder-with-the-features-you-requeste

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

#Function to get Castorux Table
def getTable(url, x):
  response = requests.get(url)
  soup = BeautifulSoup(response.text, "lxml")
  table = soup.find("table")
 
  records = []
  columns = []
  ids=[]
  redirects=[]
  for tr in table.findAll("tr"):
      ths = tr.findAll("th")
      if ths != []:
          for each in ths:
              columns.append(each.text)
      else:
          trs = tr.findAll("td")
          record = []
          for each in trs:
              try:
                  link = each.find("a")["href"]
                  text = each.text
                  record.append("https://www.castorus.com"+link)
                  record.append(text)
                  id=re.search("(\w\d+)", link).group(0)
                  ids.append(id)
                  redirect=getSourceLink("https://www.castorus.com/r.php?redirect="+id)
                  redirects.append(redirect)
              except:
                  text = each.text
                  record.append(text)
          records.append(record)
 
  columns.insert(4, "Link")
  print(records)
  print(columns)
  SingleTable = pd.DataFrame(data=records, columns = columns)
  SingleTable["id"]=ids
  SingleTable["SourceLink"]=redirects
  return SingleTable
 
#Get Source link of the apt
def getSourceLink(url):
  try:
      html = urlopen(url)
  except HTTPError as e:
      return None
  try:
      bsObj = BeautifulSoup(html.read(), "lxml")
      link = bsObj.body.script
      if link == None:
          print("SourceLink could not be found")
      else:
          SourceLinkRaw=pd.DataFrame(link).convert_dtypes().to_string()
          SourceLink = re.search("(https?:\/\/[^\"]*)", SourceLinkRaw).group(0)       
  except AttributeError as e:
      return None
  return SourceLink
 
# Get all table of the page
def FullTable(url, maxRange): 
  FullTable=pd.DataFrame()
  x=0
  for x in range(0,maxRange):
    Table=getTable(url, x)
    x=x+1
    Table['Source'] = x
    FullTable=FullTable.append(Table, ignore_index=True)
  return FullTable


#get all image for figaro source
def GetImageFromHtml(Provider, tag, classType):
  
  #Get Link Image
  g=0
  for g in range(len(Provider)):
    uClient= urlopen(Provider["SourceLink"][g])
    page_html= uClient.read()
    uClient.close()
    page_soup=BeautifulSoup(page_html, "html")
    imgs = page_soup.find_all(tag, {'class': classType})
    imgeDB=[]
    i=0
    #Get all images of apt
    for each in imgs:
      imageLink=page_soup.find_all(tag, {'class': classType})[i]
      SourceLinkRaw=str(imageLink)
      SourceLink = re.search("(https?:\/\/[^\"]*)", SourceLinkRaw).group(0)
      imgeDB.append(SourceLink)
      ImageName= Provider["id"][g] +"_"+ str([i])+".jpg"
      urllib.request.urlretrieve(SourceLink, ImageName)
      files.download(ImageName)
      i=i+1
    g=g+1

#get all image for bien ici source
def bieniciImage(BienIci, xpath, attribute):
  g=0
  for g in range(len(BienIci)):
    bienici=jsDriver(BienIci["SourceLink"][g])
    time.sleep(3)
    src = bienici.find_elements_by_xpath(xpath)
    imgeDB=[]
    i=0
    for each in src:
      SourceLink=each.get_attribute(attribute)
      imgeDB.append(SourceLink)
      print(SourceLink)
      ImageName= BienIci["id"][g] +"_"+ str([i])+".jpg"
      urllib.request.urlretrieve(SourceLink, ImageName)
      #files.download(ImageName)
      i=i+1
    g=g+1


#Use Chrome Headless Browser
def jsDriver(url):
  user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36'
  options = webdriver.ChromeOptions()
  options.add_argument('--headless')
  options.add_argument('--no-sandbox')
  options.add_argument('--disable-dev-shm-usage')
  options.add_argument("start-maximized")
  options.add_experimental_option("excludeSwitches", ["enable-automation"])
  options.add_argument(f'user-agent={user_agent}')
  options.add_experimental_option('useAutomationExtension', False)
  jsDriver =webdriver.Chrome('chromedriver',options=options)
  jsDriver.get(url)
  return jsDriver


# COMMAND ----------

##main
url = 'https://www.castorus.com/immobilier-a-levallois-perret,92300'
castorus=FullTable(url, 4)
castorus_filter=castorus[(castorus.m2.astype(float) < 40.0) & (castorus.m2.astype(float) > 25.0)].reset_index()
rem_dup_castorus=castorus.drop_duplicates(subset=['id'], keep='first').reset_index()
clean_castorus=castorus_filter.drop_duplicates(subset=['id'], keep='first')

# COMMAND ----------

url='https://www.castorus.com/s/Levallois-perret,38504,-------------------------'
castorus_max=FullTable(url, 1)
castorus_max_raw=castorus_max.rename(columns = {'m2\xa0': 'm2'}, inplace = False)
castorus_max_raw=castorus_max_raw.replace('',0)
castorus_filter_max=castorus_max_raw[(castorus_max_raw['m2'].astype(float) < 40) & (castorus_max_raw['m2'].astype(float) > 25)].reset_index()
clean_castorus_max=castorus_filter_max.drop_duplicates(subset=['id'], keep='first')
rem_dup_castorus_max=castorus_max_raw.drop_duplicates(subset=['id'], keep='first').reset_index()

##download apt
#file_id = 'ImmoDataCollection_apt_max_' + str(datetime.now()) + '.csv'
#rem_dup_castorus_max.to_csv(file_id, index=False, encoding='utf-8-sig')
#files.download(file_id)
#
##download change
#cgt_file_id = 'ImmoDataCollection_change_max_' + str(datetime.now()) + '.csv'
#cgtCastorus_max.to_csv(cgt_file_id, index=False, encoding='utf-8-sig')
#files.download(cgt_file_id)

# COMMAND ----------

clean_castorus_max=castorus_filter_max.drop_duplicates(subset=['id'], keep='first')

# COMMAND ----------

clean_castorus_max=clean_castorus_max.rename(columns = {'évol.': 'evolution', "prix \xa0": 'prix', 'vue le':'vue'}, inplace = False)
clean_castorus_max[['evolution', 'piec.']]=clean_castorus_max[['evolution', 'piec.']].astype(str)
castorus = spark.createDataFrame(clean_castorus_max)
display(castorus)
# If we need to overwrite the Schema, run this command
castorus.distinct().write.mode("overwrite").option("overwriteschena", "true").format("delta").save("/user/castorus") 
spark.sql("CREATE TABLE IF NOT EXISTS castorus USING DELTA LOCATION '/user/castorus'")

# COMMAND ----------

clean_castorus_max

# COMMAND ----------

type(clean_castorus_max)

# COMMAND ----------

castorus = spark.createDataFrame(clean_castorus_max)

# COMMAND ----------

castorus.distinct().write.mode("overwrite").option("overwriteschena", "true").format("delta").save("/user/castorus") 
spark.sql("CREATE TABLE IF NOT EXISTS castorus USING DELTA LOCATION '/user/castorus/'")

# COMMAND ----------

type(castorus)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.permid_instr I USING permid_Instr_updates
# MAGIC ON bronze.permid_instr.permid_url= permid_Instr_updates.permid_url and bronze.permid_instr.Ticker = permid_Instr_updates. Ticker and bronze.permid_instr.Mic = permid_Instr_updates.Mic
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET * WHEN NOT MATCHED THEN
# MAGIC INSERT *
