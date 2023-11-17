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
  jsDriver =webdriver.Chrome('chromedriver',options=options).add_cookie({'cookie': 'xtvrn=$490804$','xtat490804=-', 'xtant490804=1didomi_token=eyJ1c2VyX2lkIjoiMTdiYjZjNWQtN2M5Mi02YzUzLWJmZDMtMTg1N2I3OTUwYjJkIiwiY3JlYXRlZCI6IjIwMjEtMDktMDVUMTY6MjM6NDUuOTcxWiIsInVwZGF0ZWQiOiIyMDIxLTA5LTA1VDE2OjIzOjQ1Ljk3MVoiLCJ2ZW5kb3JzIjp7ImVuYWJsZWQiOlsiZ29vZ2xlIiwiYzpvbW5pdHVyZS1hZG9iZS1hbmFseXRpY3MiLCJjOmhhcnZlc3QtUFZUVHRVUDgiXX0sInB1cnBvc2VzIjp7ImVuYWJsZWQiOlsiYW5hbHlzZWRlLVZEVFVVaG42IiwicHVycG9zZV9hbmFseXRpY3MiLCJkZXZpY2VfY2hhcmFjdGVyaXN0aWNzIiwiZ2VvbG9jYXRpb25fZGF0YSJdfSwidmVuZG9yc19saSI6eyJlbmFibGVkIjpbImdvb2dsZSJdfSwidmVyc2lvbiI6MiwiYWMiOiJBa3VBRUFGa0JKWUEuQWt1QUNBa3MifQ==', 'euconsent-v2=CPMEVa0PMEVa0AHABBENBpCsAP_AAH_AAAAAILtf_X__b3_j-_59f_t0eY1P9_7_v-0zjhfdt-8N2f_X_L8X42M7vF36pq4KuR4Eu3LBIQdlHOHcTUmw6okVrzPsbk2Mr7NKJ7PEmnMbO2dYGH9_n93TuZKY7__8___z__-v_v____f_r-3_3__5_X---_e_V399zLv9____39nN___9uCCYBJhqXkAXYljgybRpVCiBGFYSHQCgAooBhaJrCBlcFOyuAj1BCwAQmoCMCIEGIKMGAQACAQBIREBIAeCARAEQCAAEAKkBCAAjYBBYAWBgEAAoBoWIEUAQgSEGRwVHKYEBEi0UE9lYAlF3saYQhllgBQKP6KjARKEECwMhIWDmOAJAS4AA.f_gAD_gAAAAA', '_gcl_au=1.1.532455363.1630859026; visitId=1630859026167-756097302', '_ga=GA1.2.1061009296.1630859026', '_gid=GA1.2.574875342.1630859026', '_hjid=baa2883f-5d9c-4b31-8dce-a03b159c9669', '_hjFirstSeen=1', '_hjAbsoluteSessionInProgress=1', 'mics_vid=9403629752', 'mics_lts=1630858969656', '_hjIncludedInSessionSample=1', '__troRUID=e6f27f7c-ce94-40e5-b7c7-8b05fb36b034', '__troSYNC=1', 'ABTasty=uid=8ythjj3jretgn0fr&fst=1630859026430&pst=-1&cst=1630859026430&ns=1&pvt=6&pvis=6&th=382147.491967.5.5.1.1.1630859026830.1630861073805.1', 'ABTastySession=mrasn=&sen=14&lp=https%253A%252F%252Fwww.logic-immo.com%252Fdetail-vente-BDF577B4-945D-912C-7E6A-D164A7AB6022.htm', '_uetsid=a5b6e3900e6511ecbb254d434e8f7f0b', '_uetvid=a5b778c00e6511ec97295d8fb39a1381','cto_bundle=XVq5al9UejdwODU1MmhwUnkzdmdncWxTZVQlMkZQeVZ4NVZUSHIzNGM0Q3FiRE9DYXdkN1FDJTJCb1VicFBGQW1UQjBXVnE3YnZneE1zRzU2YnFHVyUyQiUyQmp4SGxxdWZEb3o1b0R3RGRZJTJGWWxvTmlKMGNlRXBaNGJrUXR4UlNpWllNZTJKdGxod0swbU9UVmJXVnE3Y2FxJTJCaWFEQndaWHBUeW1oN3BVbkJSRkd4bDd6dkFqaFpMWFdKMDdEaE0lMkJRaTN1Z1B1NmNYaCUyRkZtMHJtZFQ3b055UEl3aWRVbFk4dyUzRCUzRA', '__trossion=1630860116_1800_1__e6f27f7c-ce94-40e5-b7c7-8b05fb36b034%3A1630860116_1630861078_4_','datadome=3zw84qaiuAz7VzW05soUpDPrEE.CYja2ch3DLgyDUGhz7FfDhJmFWPbFfm.pLMxZ51QttT8JzLmHWy1I66c4J8RcIeLGV4~W9~m0b4utUw'}
)
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

x=jsDriver('https://www.logic-immo.com/detail-vente-59E08435-2317-D02A-2BDA-EB54068BE267.htm')

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
