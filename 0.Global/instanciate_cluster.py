# Databricks notebook source
# MAGIC %md
# MAGIC ##Debug help
# MAGIC if packages are missing go to https://debian.pkgs.org/ 
# MAGIC some bash command:
# MAGIC sudo apt-get clean <br>
# MAGIC python -m pip install --upgrade pip <br>
# MAGIC sudo apt-get update -y <br>
# MAGIC sudo apt-get -y dist-upgrade <br>
# MAGIC sudo apt-get upgrade -y <br>
# MAGIC sudo apt-get -f install <br>
# MAGIC sudo apt autoremove -y <br>
# MAGIC apt --fix-broken install

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

# COMMAND ----------

# MAGIC %sh 
# MAGIC wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
# MAGIC sudo dpkg -i google-chrome-stable_current_amd64.deb
# MAGIC sudo apt-get install -f -y

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
