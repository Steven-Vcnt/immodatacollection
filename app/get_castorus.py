import util.scrappers as us
import pandas as pd

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
    full_main = full_main.append(sp_MainCastorus)
full_main = full_main.drop_duplicates("id")
del full_main['Unnamed: 0']
# Create SQL view
full_main["depuis"] = full_main["depuis"].astype(str)
full_main["piec."] = full_main["piec."].astype(str)
full_main["m2"] = full_main["m2"].astype(str)
spark.createDataFrame(
    full_main[full_main["SourceLink"].notnull()].reset_index(drop=True)
).distinct().createOrReplaceTempView("main_castorus_updates")

spark.sql('''
MERGE INTO bronze.main_castorus USING main_castorus_updates ON bronze.main_castorus.id = main_castorus_updates.id
WHEN MATCHED THEN
UPDATE
SET
  *
  WHEN NOT MATCHED THEN
INSERT
''')

sp_MainCastorus.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/bronze/main_castorus") 
spark.sql("CREATE TABLE IF NOT EXISTS bronze.main_castorus USING DELTA LOCATION '/FileStore/bronze/main_castorus'")


# Castorus Change table

castorus = spark.sql('''SELECT Link, mt.id
FROM silver.main_table mt
LEFT JOIN silver.meta_table md ON md.id=mt.id
where (UpdateDateChange is null OR UpdateDateChange < DATE_ADD(NOW(),-7)) ''').limit(1000).toPandas()
if len(castorus) == 0:
    dbutils.notebook.exit('Success')
else:
    pd_castorus = us.GetCastorus.getChange(castorus)
    sp_Castorus = spark.createDataFrame(pd_castorus)
    sp_Castorus.distinct().createOrReplaceTempView('castorus_change_updates')

spark.sql('''MERGE INTO bronze.castorus_change
USING castorus_change_updates
ON bronze.castorus_change.id=castorus_change_updates.id
and bronze.castorus_change.Date_MDDYYYY = castorus_change_updates.Date_MDDYYYY
and bronze.castorus_change.changeDescription =castorus_change_updates.changeDescription
WHEN MATCHED THEN
UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
''')

sp_Castorus.distinct().write.mode("overwrite").option("overwriteschema", "true").format("delta").save("/FileStore/bronze/castorus_change") 
spark.sql("CREATE TABLE IF NOT EXISTS bronze.castorus_change USING DELTA LOCATION '/FileStore/bronze/castorus_change'")