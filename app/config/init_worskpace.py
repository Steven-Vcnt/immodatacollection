# Databricks notebook source
import sys
sys.path.append("/Workspace/Repos/steven.vincent@bsb-education.com/immodatacollection/app")
import util.scrappers as us
import pandas as pd

# COMMAND ----------

# MAGIC %sh
# MAGIC sudo apt-get update
# MAGIC wget http://ftp.de.debian.org/debian/pool/main/f/fonts-liberation/fonts-liberation_1.07.4-11_all.deb
# MAGIC sudo dpkg -i fonts-liberation_1.07.4-11_all.deb
# MAGIC wget http://ftp.de.debian.org/debian/pool/main/m/mesa/libgbm1_20.3.5-1_amd64.deb
# MAGIC sudo dpkg -i libgbm1_20.3.5-1_amd64.deb
# MAGIC wget http://ftp.de.debian.org/debian/pool/main/n/nspr/libnspr4_4.29-1_amd64.deb
# MAGIC sudo dpkg -i libnspr4_4.29-1_amd64.deb
# MAGIC wget http://ftp.de.debian.org/debian/pool/main/n/nss/libnss3_3.61-1+deb11u1_amd64.deb
# MAGIC sudo dpkg -i libnss3_3.61-1+deb11u1_amd64.deb
# MAGIC sudo apt-get -f install -y
# MAGIC wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
# MAGIC sudo dpkg -i google-chrome-stable_current_amd64.deb
# MAGIC sudo apt-get install -f -y

# COMMAND ----------

spark.sql('CREATE DATABASE bronze')
spark.sql('CREATE DATABASE silver')
spark.sql('CREATE DATABASE gold')

# COMMAND ----------

# Main Castorus
url = [
    "https://www.castorus.com/s/Levallois-perret,38504,-------------------------",
    "https://www.castorus.com/s/Levallois+Perret,38504,-1---------------------------",
    "https://www.castorus.com/s/Angers,19447,-------------------------"
    ]
full_main = pd.DataFrame()
headers = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7,es;q=0.6",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
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
    sp_MainCastorus = pd.DataFrame(us.GetCastorus.CastorusMainTable(each, headers).rename(columns={"vue le": "vue"}, inplace=False))
    full_main = pd.concat([full_main, sp_MainCastorus])
full_main = full_main.drop_duplicates("id")
full_main['rendmt_brut'] = full_main['rendmt brut']
del full_main['rendmt brut']
del full_main['Unnamed: 0']
# Create SQL view
full_main["depuis"] = full_main["depuis"].astype(str)
full_main["piec."] = full_main["piec."].astype(str)
full_main["m2"] = full_main["m2"].astype(str)
sp_MainCastorus=spark.createDataFrame(
    full_main[full_main["SourceLink"].notnull()].reset_index(drop=True)
)
sp_MainCastorus.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/bronze/main_castorus") 
spark.sql("CREATE TABLE IF NOT EXISTS bronze.main_castorus USING DELTA LOCATION '/FileStore/bronze/main_castorus'")


# COMMAND ----------


# Castorus Change table

castorus = spark.sql('''SELECT mc.Link, mc.id
FROM bronze.main_castorus mc
''').limit(1000).toPandas()
if len(castorus) == 0:
    dbutils.notebook.exit('Success')
else:
    pd_castorus = us.GetCastorus.getChange(castorus)
    sp_Castorus = spark.createDataFrame(pd_castorus)

sp_Castorus.distinct().write.mode("overwrite").option("overwriteschema", "true").format("delta").save("/FileStore/bronze/castorus_change") 
spark.sql("CREATE TABLE IF NOT EXISTS bronze.castorus_change USING DELTA LOCATION '/FileStore/bronze/castorus_change'")

# COMMAND ----------

pd_castorus

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM bronze.castorus_change

# COMMAND ----------


