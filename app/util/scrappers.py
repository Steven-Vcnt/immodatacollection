import pandas as pd
import requests
import urllib.request
import time
import lxml
import os
import bs4.builder._lxml
import lxml.etree
import re
from selenium import webdriver
from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.error import HTTPError
from datetime import datetime
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
pd.options.display.max_colwidth = 1000


class GetCastorus:
    def CastorusMainTable(url, headers):
        '''
        We extract from Castorus the main table available on the website. 
        We store ad information to use as an input for other scrapper functions
        '''
        ids = []
        redirects = []
        # read table with the id myTableResult
        mainCastorus = GetCastorus.jsDriver(url)
        time.sleep(5)
        Maintable = pd.read_html(mainCastorus.page_source, attrs={"id": "table-result"})
        # Maintable = pd.read_html(
        #    requests.get(url, headers=headers).text
        # )
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
                    redirect = GetCastorus.getSourceLink(
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

    def getChange(Table):
        '''
        Extract changes available in castorus
        '''
        ChangeTable = pd.DataFrame()
        for link, id in zip(Table["Link"], Table["id"]):
            df_change = pd.DataFrame()
            # read URL and get html
            try:
                html_change = pd.read_html(link, attrs={'width': '98%'})
                # convert from list to DF
                df_change = pd.concat(html_change)   
                df_change['link'] = link
                df_change['id'] = id
                df_change['UpdateDateChange'] = datetime.now()
                df_change[0] = df_change[0].astype(str)
            except:
                df_change = pd.DataFrame({0: ['1011999'], 1: ['not available'], 'id': [id], 'UpdateDateChange': [datetime.now()]})
                ChangeTable = ChangeTable.append(df_change)
                # Regex to get ID
        ChangeTable.rename(columns={0: 'Date_MDDYYYY', 1: 'changeDescription'}, inplace=True)
        return ChangeTable

    def getSourceLink(url, headers):
        try:
            # get URL HTML
            html = requests.get(url, headers=headers)
        except HTTPError:
            return None
        try:
            # Set up BS4 function
            bsObj = BeautifulSoup(html.text, "lxml")
            link = bsObj.body.script
            if link is None:
                SourceLink = 'Not Found'
                print("SourceLink could not be found")
            else:
                # Convert link to dataframe
                SourceLinkRaw = pd.DataFrame(link).convert_dtypes().to_string()
                # REGEX to extract url SOurce Link
                SourceLink = re.search('(https?:\/\/[^"]*)', SourceLinkRaw).group(0)
        except AttributeError as e:
            SourceLink = 'Not Found'
            return print(e)
        return SourceLink


class driver:
    def jsDriver(url):
        '''
        Use Chrome Headless Browser
        '''
        user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36"
        service = Service()
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("start-maximized")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_argument(f"user-agent={user_agent}")
        options.add_experimental_option("useAutomationExtension", False)
        jsDriver = webdriver.Chrome(service=service, options=options)
        jsDriver.get(url)
        return jsDriver


class getSource:
    def bieniciImage(Provider, xpath, attribute):
        '''
        get all image for bien ici source
        '''
        imageTable = pd.DataFrame()
        for link, id in zip(Provider["SourceLink"], Provider["id"]):
            imgeTable_temp = pd.DataFrame()
            bienici = driver.jsDriver(link)
            time.sleep(3)
            src = bienici.find_elements(By.XPATH, xpath)
            # .find_elements_by_xpath(xpath)
            imgeDB = []
            imagePath = []
            i = 0
            imageLink = 'not available'
            imageName = 'not available'
            for each in src:
                imageLink = each.get_attribute(attribute)
                imgeDB.append(imageLink)
                imageName = "dbfs:/FileStore/images/" + id + "_" + str(i)+".jpg"
                imagePath.append(imageName)
                try:
                    urllib.request.urlretrieve(imageLink, "/tmp/imageTempbi.jpg")
                    dbutils.fs.mv("file:/tmp/imageTempbi.jpg", imageName)
                except:
                    pass
                else:
                    pass
                i = i+1
            if imageLink == 'not available':
                imgeDB.append(imageLink)
                imagePath.append(imageName)
            else:
                pass
            imgeTable_temp['imageLink'] = imgeDB
            imgeTable_temp['id'] = id
            imgeTable_temp['imagePath'] = imagePath
            imageTable = imageTable.append(imgeTable_temp)
        imageTable['UpdateDateImage'] = datetime.now()
        return imageTable

    def GetImageFromHtml(Provider, tag, classType):
        imageTable = pd.DataFrame()
        # Get Link Image
        for link, id in zip(Provider["SourceLink"], Provider["id"]):
            imgeTable_temp = pd.DataFrame()
            try:
                uClient = urlopen(link)
                page_html = uClient.read()
                uClient.close()
                page_soup = BeautifulSoup(page_html, "html")
                imgs = page_soup.find_all(tag, {'class': classType})
            except:
                imgs = []
            else:
                imgs = []

            i = 0
            imgeDB = []
            imagePath = []
            imageLink = 'not available'
            imageName = 'not available'
            # Get all images of apt
            for each in imgs:
                imageLink = re.search("(https?:\/\/[^\"]*)", str(each)).group(0)
                imgeDB.append(imageLink)
                imageName = "dbfs:/FileStore/images/" + id + "_" + str(i)+".jpg"
                imagePath.append(imageName)
                try:
                    urllib.request.urlretrieve(imageLink, "/tmp/imageTemp.jpg")
                    dbutils.fs.mv("file:/tmp/imageTemp.jpg", imageName)
                except:
                    pass
                else:
                    pass
                i = i+1
            if imageLink == 'not available':
                imgeDB.append(imageLink)
                imagePath.append(imageName)
            else:
                pass
            imgeTable_temp['imageLink'] = imgeDB
            imgeTable_temp['id'] = id
            imgeTable_temp['imagePath'] = imagePath
            imageTable = imageTable.append(imgeTable_temp)

        imageTable['UpdateDateImage'] = datetime.now()
        return imageTable
