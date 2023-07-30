FROM postgres

COPY data/reference.csv /data/reference.csv
COPY setup/create_table.sql /docker-entrypoint-initdb.d/