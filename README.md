# OpenNeurons

sudo -u postgres psql   -tc  "SELECT 1 FROM pg_database WHERE datname = 'trmdb'" | grep -q 1 || sudo -u postgres psql -tc "CREATE DATABASE trmdb"
sudo -u postgres psql -d trmdb < fisher.sql

