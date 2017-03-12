# OpenNeurons

debuild -d && sudo dpkg -i ../open-trm_99999.9_amd64.deb


sudo -u postgres psql   -tc  "SELECT 1 FROM pg_database WHERE datname = 'trmdb'" | grep -q 1 || sudo -u postgres psql -tc "CREATE DATABASE trmdb"

sudo -u postgres psql trmdb < fish
