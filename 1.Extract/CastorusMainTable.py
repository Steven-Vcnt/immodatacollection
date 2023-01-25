# Databricks notebook source
# DBTITLE 1,Installation package
import pandas as pd
import requests
from selenium import webdriver
from bs4 import BeautifulSoup

pd.options.display.max_colwidth = 1000
from urllib.request import urlopen
from urllib.error import HTTPError
import re
from datetime import datetime
import urllib.request
import time
import lxml
import bs4.builder._lxml
import lxml.etree
import html5lib
from selenium.webdriver.common.by import By

# COMMAND ----------

# DBTITLE 1,Function definition
# Get full table from url
def CastorusMainTable(url, headers):
    ids = []
    redirects = []
    # read table with the id myTableResult
    mainCastorus=jsDriver(url)
    time.sleep(5)
    Maintable=pd.read_html(mainCastorus.page_source, attrs={"id": "table-result"})
    #Maintable = pd.read_html(
    #    requests.get(url, headers=headers).text
    #)
    # convert list to dataframe
    df_table = pd.concat(Maintable)
    # Get URL HTML source page
    response = mainCastorus.page_source
    # setup BS4 function
    soup = BeautifulSoup(response, "html.parser")
    # Fetch table in HTML
    table = soup.find("table")
    links = []
    # For each row, extract URL link to apartment info
    for tr in table.findAll("tr"):
        trs = tr.findAll("td")
        for each in trs:
            try:
                # We extract the URL from A HREF
                link = each.find("a")["href"]
                # We reconstruct the full URL
                links.append("https://www.castorus.com" + link)
                # Regex to extract the aprtment ID
                id = link.split(",", 1)[1]
                ids.append(id)
                # We reconstruct the redirection link from the id and the function Source Link
                redirect = getSourceLink(
                    "https://www.castorus.com/r.php?redirect=" + id, headers
                )
                redirects.append(redirect)
            except:
                pass
    df_table["id"] = ids
    df_table["SourceLink"] = redirects
    df_table["Link"] = links
    df_table["UpdateDateMainTable"] = datetime.now()
    return df_table


# Get Source link of the apt
def getSourceLink(url, headers):
    try:
        # get URL HTML
        html = requests.get(url, headers=headers)
    except HTTPError as e:
        return None
    try:
        # Set up BS4 function
        bsObj = BeautifulSoup(html.text, "lxml")
        link = bsObj.body.script
        if link == None:
            SourceLink='Not Found'
            print("SourceLink could not be found")
        else:
            # Convert link to dataframe
            SourceLinkRaw = pd.DataFrame(link).convert_dtypes().to_string()
            # REGEX to extract url SOurce Link
            SourceLink = re.search('(https?:\/\/[^"]*)', SourceLinkRaw).group(0)
    except AttributeError as e:
        return print(e)
    return SourceLink


# Use Chrome Headless Browser
def jsDriver(url):
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36"
    options = webdriver.ChromeOptions()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("start-maximized")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_argument(f"user-agent={user_agent}")
    options.add_experimental_option("useAutomationExtension", False)
    jsDriver = webdriver.Chrome("chromedriver", options=options)
    jsDriver.get(url)
    return jsDriver

# COMMAND ----------

# DBTITLE 1,Main
url = [
    "https://www.castorus.com/s/Levallois-perret,38504,-------------------------",
    "https://www.castorus.com/s/Levallois+Perret,38504,-1---------------------------",
]  #'https://www.castorus.com/s/Angers,19447,-------------------------', 'https://www.castorus.com/s/Evian+Les+Bains,32605,-1---------------------------']
full_main = pd.DataFrame()
headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7,es;q=0.6",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    #'Cookie': 'PHPSESSID=t68itgs74h7re2o7d3p2nh2t91; _ga=GA1.2.1507887170.1630858402; __utmc=240501009; __utma=240501009.1507887170.1630858402.1658066801.1671635325.5; __utmz=240501009.1671635325.5.2.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); __utmt=1; _gid=GA1.2.168696422.1671635325; euconsent-v2=CPkVv0APkVv0ABcAIEFRCvCgAP_AAH_AAAqIIyQMQABQAKAAsAB4AFQAMgAgABUAC2AGgAawBEAEWAJgAmgBbgDCAMQAcoBBgEIAJgAToApABcADHAHoAP0AgYBCACOgE8AKuAXUAwIBhADOgGiANeAbQBHYCPQEvAJiAT-AowBcwC8wGLgMZAZIA5MB1AD0gIDgjJAdAAWAA8ACoAGQAQQA0ADWAIgAigBMADEAH4AQgAmAB-gEDAIQARYAjoBVwC6gGBANEAa8A2gCPQExALzAZJA5MDlAHpAAAA.x8i-nkQ.YKgAAAAAAAA.4YpAPABZAC6AGwAYgBEACjAHFAPWAksB6oERAIkgSiAliBLUCZwGFgOZAdOBIZCgsKDAUIgp1BWeCzMFo4LfQXOgvJBikAEIAcACAAKgAyADQAIQAhABgAQABQA; u_Ca=1; lastidcomconsult=38504; __utmb=240501009.12.10.1671635325',
    "Host": "www.castorus.com",
    "Pragma": "no-cache",
    "Referer": "https://www.castorus.com/immobilier-a-levallois-perret,92300",
    "sec-ch-ua": '"Not?A_Brand";v="8", "Chromium";v="108", "Google Chrome";v="108"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36",
}
for each in url:
    # Convert Pandas DF to SPark DF, get Castorus Main table and rename column
    sp_MainCastorus = pd.DataFrame(
        CastorusMainTable(each, headers).rename(
            columns={"vue le": "vue"}, inplace=False
        )
    )
    full_main = full_main.append(sp_MainCastorus)
full_main = full_main.drop_duplicates("id")
del full_main['Unnamed: 0']
# Create SQL view
full_main["depuis"]=full_main["depuis"].astype(str)
full_main["piec."]=full_main["piec."].astype(str)
full_main["m2"]=full_main["m2"].astype(str)
spark.createDataFrame(
    full_main[full_main["SourceLink"].notnull()].reset_index(drop=True)
).distinct().createOrReplaceTempView("main_castorus_updates")

# COMMAND ----------

# DBTITLE 1,Upsert
# MAGIC %sql MERGE INTO bronze.main_castorus USING main_castorus_updates ON bronze.main_castorus.id = main_castorus_updates.id
# MAGIC WHEN MATCHED THEN
# MAGIC UPDATE
# MAGIC SET
# MAGIC   *
# MAGIC   WHEN NOT MATCHED THEN
# MAGIC INSERT
# MAGIC   *

# COMMAND ----------

# DBTITLE 1,Overwrite file & Create delta table
#sp_MainCastorus.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/bronze/main_castorus") 
#spark.sql("CREATE TABLE IF NOT EXISTS bronze.main_castorus USING DELTA LOCATION '/FileStore/bronze/main_castorus'")
