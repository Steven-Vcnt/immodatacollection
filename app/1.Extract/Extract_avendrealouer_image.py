# Databricks notebook source
import pandas as pd
import requests
from selenium import webdriver
from bs4 import BeautifulSoup
pd.options.display.max_colwidth=1000
from urllib.request import urlopen, Request
from urllib.error import HTTPError
import re
from datetime import datetime
import urllib.request
import time
import lxml
import bs4.builder._lxml
import lxml.etree
import requests

# COMMAND ----------

#get all image for aval source
def GetImageFromHtml(Provider, tag, classType):
  imageTable=pd.DataFrame()
  #Get Link Image
  
  for link, id in zip(Provider["SourceLink"], Provider["id"]):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10.13; rv:63.0) Gecko/20100101 Firefox/63.0'}
    imgeTable_temp=pd.DataFrame()
    req= Request(link, headers=headers)
    try:
      uClient= urlopen(req)
      page_html= uClient.read()
      uClient.close()
      page_soup=BeautifulSoup(page_html, "html")
      imgs = page_soup.find_all(tag, {'class': classType})
    except:
      imgs=[]
    i=0
    imgeDB=[]
    imagePath=[]
    imageLink='not available'
    imageName='not available'
    #Get all images of apt
    for each in imgs:
      #imageLink_raw=page_soup.find_all(tag, {'class': classType})[i]
      imageLink = re.search("(https?:\/\/[^\"]*)", str(each)).group(0)
      imgeDB.append(imageLink)
      
      ImageName= "dbfs:/FileStore/images/"+ id +"_"+ str(i)+".jpg"
      imagePath.append(ImageName)
      try:
        urllib.request.urlretrieve(imageLink, "/tmp/imageTemp3.jpg", headers=headers)
        dbutils.fs.mv("file:/tmp/imageTemp3.jpg", ImageName)
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

#download A vendre a louer
aval_apt=spark.sql('''SELECT SourceLink, mt.id 
FROM silver.main_table mt
LEFT JOIN silver.meta_table md ON md.id=mt.id
where SourceLink LIKE '%avendrealouer%' and (UpdateDateImage is null OR UpdateDateImage < DATE_ADD(NOW(),-7)) ''').limit(1000).toPandas()
if len(aval_apt) == 0:
  dbutils.notebook.exit('Success')
else:
  full_image=GetImageFromHtml(aval_apt, 'img', 'SliderImages__ImgStyledSlider-sc-18z29ar-5 esrCRM adviewPhoto')
  sp_avalImage=spark.createDataFrame(full_image)
  sp_avalImage.distinct().createOrReplaceTempView('aval_image_updates')


# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO bronze.aval_image
# MAGIC USING aval_image_updates
# MAGIC ON bronze.aval_image.id=aval_image_updates.id
# MAGIC and bronze.aval_image.imageLink = aval_image_updates.imageLink
# MAGIC and bronze.aval_image.imagePath = aval_image_updates.imagePath
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

#sp_avalImage.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/bronze/avalImage") 
#spark.sql("CREATE TABLE IF NOT EXISTS bronze.aval_image USING DELTA LOCATION '/FileStore/bronze/avalImage'")
