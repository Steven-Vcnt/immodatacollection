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

# COMMAND ----------

# MAGIC %sh
# MAGIC wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
# MAGIC sudo dpkg -i google-chrome-stable_current_amd64.deb
# MAGIC sudo apt-get install -f -y
# MAGIC sudo dpkg -i google-chrome-stable_current_amd64.deb

# COMMAND ----------

# MAGIC %sh
# MAGIC CHROME_VERSION=$(google-chrome --version | cut -f 3 -d ' ' | cut -d '.' -f 1) \
# MAGIC         && CHROMEDRIVER_RELEASE=$(curl --location --fail --retry 3 http://chromedriver.storage.googleapis.com/LATEST_RELEASE_${CHROME_VERSION}) \
# MAGIC         && curl --silent --show-error --location --fail --retry 3 --output /tmp/chromedriver_linux64.zip "http://chromedriver.storage.googleapis.com/$CHROMEDRIVER_RELEASE/chromedriver_linux64.zip" \
# MAGIC         && cd /tmp \
# MAGIC         && unzip chromedriver_linux64.zip \
# MAGIC         && rm -rf chromedriver_linux64.zip \
# MAGIC         && sudo mv chromedriver /usr/local/bin/chromedriver \
# MAGIC         && sudo chmod +x /usr/local/bin/chromedriver \
# MAGIC         && chromedriver --version

# COMMAND ----------

#Return a evalue when using dbutils.notebook.run for orchestration
dbutils.notebook.exit('Success')
