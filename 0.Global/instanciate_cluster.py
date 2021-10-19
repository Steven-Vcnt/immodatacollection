# Databricks notebook source
# MAGIC %sh
# MAGIC python -m pip install --upgrade pip
# MAGIC sudo apt-get update -y
# MAGIC sudo apt-get -y dist-upgrade
# MAGIC sudo apt-get upgrade -y
# MAGIC sudo apt-get -f install
# MAGIC sudo apt autoremove -y
# MAGIC sudo apt-get install chromium-browser -y
# MAGIC sudo apt-get install chromium-chromedriver -y
# MAGIC #pip install pandas
# MAGIC #pip install lxml
# MAGIC #pip install -q selenium
# MAGIC #pip install Pillow
# MAGIC #pip install html5
# MAGIC #pip install -q beautifulsoup4 

# COMMAND ----------

#Return a evalue when using dbutils.notebook.run for orchestration
dbutils.notebook.exit('Success')
