FROM postgres

ENV POSTGRES_USER khuyentran
ENV POSTGRES_PASSWORD 123456
ENV POSTGRES_DB monitoring_db

COPY data/reference.csv /data/reference.csv
COPY src/setup/create_table.sql /docker-entrypoint-initdb.d/