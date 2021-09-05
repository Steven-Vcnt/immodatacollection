# Databricks notebook source
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

#get all image for bien ici source
def bieniciImage(Provider, xpath, attribute):
  imageTable=pd.DataFrame()
  for link, id in zip(Provider["SourceLink"], Provider["id"]):
    imgeTable_temp=pd.DataFrame()
    bienici=jsDriver(link)
    time.sleep(3)
    src = bienici.find_elements_by_xpath(xpath)
    imgeDB=[]
    imagePath=[]
    i=0
    imageLink='not available'
    imageName='not available'
    for each in src:
      imageLink=each.get_attribute(attribute)
      imgeDB.append(imageLink)
      imageName= "dbfs:/FileStore/images/"+ id +"_"+ str(i)+".jpg"
      imagePath.append(imageName)
      try:
        urllib.request.urlretrieve(imageLink, "/tmp/imageTempbi.jpg")
        dbutils.fs.mv("file:/tmp/imageTempbi.jpg", imageName)
      except:
        pass
      else:
        pass
      i=i+1
    if imageLink=='not available':
      imgeDB.append(imageLink)
      imagePath.append(imageName)
    else:
      pass
    imgeTable_temp['imageLink']=imgeDB
    imgeTable_temp['id']=id
    imgeTable_temp['imagePath']= imagePath
    imageTable=imageTable.append(imgeTable_temp)
  imageTable['UpdateDateImage']= datetime.now()
  return imageTable
    
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
  jsDriver.implicitly_wait(15)
  jsDriver.get(url)
  return jsDriver

# COMMAND ----------

#download Le figaro
figaro_apt=spark.sql('''SELECT SourceLink, mt.id 
FROM silver.main_table mt
LEFT JOIN silver.meta_table md ON md.id=mt.id
where SourceLink LIKE '%logic_immo%' --and (UpdateDateImage is null OR UpdateDateImage < DATE_ADD(NOW(),-7)) ''').limit(100).toPandas()
if len(figaro_apt) == 0:
  dbutils.notebook.exit('Success')
else:
  full_image=GetImageFromHtml(figaro_apt, 'a', 'image-link default-image-background')
  sp_figaroImage=spark.createDataFrame(full_image)
  sp_figaroImage.distinct().createOrReplaceTempView('figaro_image_updates')

# COMMAND ----------

from selenium.webdriver.support.ui import WebDriverWait

# COMMAND ----------

x=WebDriverWait(jsDriver('https://translate.google.com/translate?sl=auto&tl=en&u=https://www.logic-immo.com/detail-vente-F2C0C1B2-CB3E-5BD0-8FAD-191C1FFB7A1D.htm'), 3)

# COMMAND ----------

x=jsDriver('https://translate.googleusercontent.com/translate_p?sl=auto&tl=en&u=https://www.logic-immo.com/detail-vente-F2C0C1B2-CB3E-5BD0-8FAD-191C1FFB7A1D.htm&depth=1&rurl=translate.google.com&sp=nmt4&pto=aue,ajax,boq&usg=ALkJrhgAAAAAYNT6i3SB9HvesQYNDBD8kjyVH7Nf7-l2')

# COMMAND ----------

print(x.page_source)

# COMMAND ----------

# MAGIC %md
# MAGIC <iframe sandbox="allow-same-origin allow-forms allow-scripts allow-popups" src="https://translate.googleusercontent.com/translate_p?sl=auto&amp;tl=en&amp;u=https://www.logic-immo.com/detail-vente-F2C0C1B2-CB3E-5BD0-8FAD-191C1FFB7A1D.htm&amp;depth=1&amp;rurl=translate.google.com&amp;sp=nmt4&amp;pto=aue,ajax,boq&amp;usg=ALkJrhgAAAAAYNT6i3SB9HvesQYNDBD8kjyVH7Nf7-l2" name="c" frameborder="0" style="height:100%;width:100%;position:absolute;top:0px;bottom:0px;"></iframe>

# COMMAND ----------

options = webdriver.ChromeOptions() 
options.add_argument("start-maximized")
options.add_experimental_option("excludeSwitches", ["enable-automation"])
options.add_experimental_option('useAutomationExtension', False)
driver = webdriver.Chrome(options=options, executable_path=r'C:\WebDrivers\chromedriver.exe')
driver.get('https://www.leboncoin.fr/')
print(driver.page_source)

# COMMAND ----------

uClient= urlopen('https://translate.googleusercontent.com/translate_p?sl=auto&tl=en&u=https://www.logic-immo.com/detail-vente-F2C0C1B2-CB3E-5BD0-8FAD-191C1FFB7A1D.htm&depth=1&rurl=translate.google.com&sp=nmt4&pto=aue,ajax,boq&usg=ALkJrhgAAAAAYNT6i3SB9HvesQYNDBD8kjyVH7Nf7-l2')
time.sleep(3)
page_html= uClient.read()
uClient.close()
page_soup=BeautifulSoup(page_html, "html")
