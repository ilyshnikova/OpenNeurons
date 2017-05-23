# OpenNeurons

## Prerequirements
```
# Check for prerequirements installed
# (not implemented yet)
python3 ./manage_server.py checkdeps

```

## Database Initialization
```
# Create an empty database
psql -U postgres -tc  "SELECT 1 FROM pg_database WHERE datname = 'trmdb'" | grep -q 1 || psql -U postgres -tc "CREATE DATABASE trmdb"

# Create empty database schema
python3 ./manage_server.py initdb

# Fill database with provided Iris Fisher data
psql -U postgres -d trmdb < fisher.sql

```

## Web server usage:
```
# Start a web server
python3 ./manage_server.py runserver

# Then open http://localhost:50001/ in your browser
```

