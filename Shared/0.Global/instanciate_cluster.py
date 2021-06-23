# Databricks notebook source
# MAGIC %sh
# MAGIC apt-get update
# MAGIC apt install chromium-chromedriver -y
# MAGIC #pip install pandas
# MAGIC #pip install lxml
# MAGIC #pip install -q selenium
# MAGIC #pip install Pillow
# MAGIC #pip install html5
# MAGIC #pip install -q beautifulsoup4 

# COMMAND ----------

#Return a evalue when using dbutils.notebook.run for orchestration
dbutils.notebook.exit('Success')
