sudo apt-get update
wget http://ftp.de.debian.org/debian/pool/main/f/fonts-liberation/fonts-liberation_1.07.4-11_all.deb
sudo dpkg -i fonts-liberation_1.07.4-11_all.deb
wget http://ftp.de.debian.org/debian/pool/main/m/mesa/libgbm1_20.3.5-1_amd64.deb
sudo dpkg -i libgbm1_20.3.5-1_amd64.deb
wget http://ftp.de.debian.org/debian/pool/main/n/nspr/libnspr4_4.29-1_amd64.deb
sudo dpkg -i libnspr4_4.29-1_amd64.deb
wget http://ftp.de.debian.org/debian/pool/main/n/nss/libnss3_3.61-1+deb11u1_amd64.deb
sudo dpkg -i libnss3_3.61-1+deb11u1_amd64.deb
sudo apt-get -f install -y

wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo dpkg -i google-chrome-stable_current_amd64.deb
sudo apt-get install -f -y

CHROME_VERSION=$(google-chrome --version | cut -f 3 -d ' ' | cut -d '.' -f 1) \
        && CHROMEDRIVER_RELEASE=$(curl --location --fail --retry 3 http://chromedriver.storage.googleapis.com/LATEST_RELEASE_${CHROME_VERSION}) \
        && curl --silent --show-error --location --fail --retry 3 --output /tmp/chromedriver_linux64.zip "http://chromedriver.storage.googleapis.com/$CHROMEDRIVER_RELEASE/chromedriver_linux64.zip" \
        && cd /tmp \
        && unzip chromedriver_linux64.zip \
        && rm -rf chromedriver_linux64.zip \
        && sudo mv chromedriver /usr/local/bin/chromedriver \
        && sudo chmod +x /usr/local/bin/chromedriver \
        && chromedriver --version