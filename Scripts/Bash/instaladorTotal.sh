#/bin/bash
cd /tmp/
add-apt-repository ppa:peek-developers/stable
apt update
apt install -y -f obs-studio vim flameshot gnome-tweaks peek wine winetricks libgtkglext1 libpangox-1.0-0 samba vlc default-jre net-tools ubuntu-restricted-extras unrar libavcodec-extra gnome-software
/usr/share/doc/libdvdread4/install-css.sh
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb -O chrome.deb
dpkg -i chrome.deb
wget https://download.anydesk.com/linux/anydesk_6.0.1-1_amd64.deb -O anydesk.deb
dpkg -i anydesk.deb
snap install spotify opera wps-office-all-lang-no-internet
cd /home/user/SPONTE
tar zxvf 1604673623_bpagent-1.0.7.tar.gz
sudo chmod +x install.sh
./install.sh
sudo adduser informaq
sudo usermod -aG sudo informaq
apt upgrade -y
apt dist-upgrade
echo "-----> INSTALAÇÃO FINALIZADA <-----"
#ftp://vpn.informaq.net.br:1981