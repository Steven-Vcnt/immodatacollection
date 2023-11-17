import pandas as pd
import requests
import urllib.request
import time
import lxml
import os
import bs4.builder._lxml
import lxml.etree
import html5lib
import re
from selenium import webdriver
from bs4 import BeautifulSoup
from urllib.request import urlopen
from urllib.error import HTTPError
from datetime import datetime
from selenium.webdriver.common.by import By
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
                except GetCastorus.notExistantTable:
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
            except GetCastorus.notavailable:
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


class getDVF:
    def getDvfFiles():
        link = 'https://www.data.gouv.fr/fr/datasets/demandes-de-valeurs-foncieres/'
        uClient = urlopen(link)
        page_html = uClient.read()
        uClient.close()
        page_soup = BeautifulSoup(page_html, "html")
        main_file = page_soup.find_all('a', {'class': 'fr-btn fr-btn--sm fr-icon-download-line matomo_download'})
        dvf_files = []
        for i in range(6):
            dvf_links = re.search("(https?:\/\/[^\"]*)", str(main_file[i])).group(0)
            dvf_files.append(dvf_links)
        dvf_files = list(dict.fromkeys(dvf_files))
        for link in dvf_files:
            fileId = re.search("(?!(.+)?\/\/?(\?.+)?).*", link).group(0)
            fileName = os.path.join("dbfs:/FileStore/DVF_file/", fileId, ".txt")
            try:
                urllib.request.urlretrieve(link, "/tmp/dvf_file.txt")
                dbutils.fs.mv("file:/tmp/dvf_file.txt", fileName)
            except getDVF.noFileAvailable:
                pass
            else:
                pass
        urllib.request.urlretrieve(link, "/tmp/dvf_file.txt")
        dbutils.fs.mv("file:/tmp/dvf_file.txt", fileName)


