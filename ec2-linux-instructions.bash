
# Setup python3
sudo yum install python3-devel
curl https://packages.microsoft.com/config/rhel/8/prod.repo > sudo /etc/yum.repos.d/mssql-release.repo
sudo ACCEPT_EULA=Y yum install -y msodbcsql17

# Add this record to cronwtab -e
@reboot cd /home/ec2-user/src && /usr/bin/python3 main.py > ../logs/`date +\%Y-\%m-\%d-\%H-\%M`-cron.log 2>&1 && sudo shutdown -P +5
