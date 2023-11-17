from bs4 import BeautifulSoup
from urllib.request import urlopen
import re
import urllib.request
from pyspark.sql import functions as F
from pyspark.sql.functions import regexp_replace


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
    fileName = "dbfs:/FileStore/DVF_file/" + fileId + ".txt"
    try:
        urllib.request.urlretrieve(link, "/tmp/dvf_file.txt")
        dbutils.fs.mv("file:/tmp/dvf_file.txt", fileName)
    except:
        pass
    else:
        pass
urllib.request.urlretrieve(link, "/tmp/dvf_file.txt")
dbutils.fs.mv("file:/tmp/dvf_file.txt", fileName)

sp_dvf_file = spark.read.format("csv").option("delimiter", "|").option("header", "true").load("/FileStore/DVF_file/*.txt")
renamed_sp_dvf = sp_dvf_file.select([F.col(col).alias(col.replace(' ', '_')) for col in sp_dvf_file.columns])
dot_sp_dvf = renamed_sp_dvf.withColumn('Valeur_fonciere', regexp_replace('Valeur_fonciere', ',', '.').cast("int"))
dot_sp_dvf = dot_sp_dvf.withColumn('Surface_reelle_bati', dot_sp_dvf['Surface_reelle_bati'].cast("int"))

spark.sql('DROP TABLE IF EXISTS bronze.dvf_file')

dot_sp_dvf.distinct().write.mode("Overwrite").option("OverwriteSchema", "true").format("delta").save("/FileStore/bronze/dvf_file") 
spark.sql("""
CREATE TABLE IF NOT EXISTS bronze.dvf_file
USING DELTA LOCATION '/FileStore/bronze/dvf_file'
""")


# # TO DEBUG and explore dataset
# renamed_sp_dvf.createOrReplaceTempView("DVF_create_table")
# %sql
# SELECT md5( CONCAT(Valeur_fonciere,'|',Date_mutation,'|',Surface_reelle_bati)) as DVF_ID, NOW() as CreatedDate, * FROM DVF_create_table where Valeur_fonciere is not  null and Surface_reelle_bati is not null and Type_local = 'Appartement'
