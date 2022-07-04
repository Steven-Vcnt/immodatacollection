# Databricks notebook source
# MAGIC %md
# MAGIC Toutes les annonces: https://www.entreparticuliers.com/annonces-immobilieres/appartement/vente/levallois-perret-92300
# MAGIC <br>
# MAGIC Tous les prix: https://www.entreparticuliers.com/prix-immobiliers/vente/levallois-perret-92300?page=500

# COMMAND ----------

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

r= requests.get("https://www.entreparticuliers.com/annonces-immobilieres/appartement/vente/levallois-perret-92300")

# COMMAND ----------

import pprint as pp
pp.pprint(r.content)

# COMMAND ----------

uClient= urlopen("https://www.entreparticuliers.com/annonces-immobilieres/appartement/vente/levallois-perret-92300")
page_html= uClient.read()
uClient.close()
page_soup=BeautifulSoup(page_html, "html")
img = page_soup.find_all("app-resultat", {'class': 'flex-grow-1 d-flex flex-column mb-5'})

# COMMAND ----------

pp.pprint(img)

# COMMAND ----------

a _ngcontent-appserver-c79

# COMMAND ----------

#get all image for figaro source
def GetImageFromHtml(Provider, tag, classType):
  imageTable=pd.DataFrame()
  #Get Link Image
  
  for link, id in zip(Provider["SourceLink"], Provider["id"]):
    imgeTable_temp=pd.DataFrame()
    try:
      uClient= urlopen(link)
      page_html= uClient.read()
      uClient.close()
      page_soup=BeautifulSoup(page_html, "html")
      imgs = page_soup.find_all(tag, {'class': classType})
    except:
      imgs=[]
    else:
      imgs=[]
      
    i=0
    imgeDB=[]
    imagePath=[]
    imageLink='not available'
    imageName='not available'
    #Get all images of apt
    for each in imgs:
      imageLink = re.search("(https?:\/\/[^\"]*)", str(each)).group(0)
      imgeDB.append(imageLink)
      imageName= "dbfs:/FileStore/images/"+ id +"_"+ str(i)+".jpg"
      imagePath.append(imageName)
      try:
        urllib.request.urlretrieve(imageLink, "/tmp/imageTemp.jpg")
        dbutils.fs.mv("file:/tmp/imageTemp.jpg", imageName)
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

# COMMAND ----------

#download Le figaro
figaro_apt=spark.sql('''SELECT SourceLink, mt.id 
FROM silver.main_table mt
LEFT JOIN silver.meta_table md ON md.id=mt.id
where SourceLink LIKE '%figaro%' and (UpdateDateImage is null OR UpdateDateImage < DATE_ADD(NOW(),-7)) ''').limit(100).toPandas()
if len(figaro_apt) == 0:
  dbutils.notebook.exit('Success')
else:
  full_image=GetImageFromHtml(figaro_apt, 'a', 'image-link default-image-background')
  sp_figaroImage=spark.createDataFrame(full_image)
  sp_figaroImage.distinct().createOrReplaceTempView('figaro_image_updates')

