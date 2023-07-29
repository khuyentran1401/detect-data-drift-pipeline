CREATE TABLE current 
(
    instant bigint,
    dteday timestamp without time zone,
    season bigint,
    yr bigint,
    mnth bigint,
    holiday bigint,
    weekday bigint,
    workingday bigint,
    weathersit bigint,
    temp double precision,
    atemp double precision,
    hum double precision,
    windspeed double precision,
    casual bigint,
    registered bigint,
    cnt bigint
);


CREATE TABLE reference
(
    instant bigint,
    dteday timestamp without time zone,
    season bigint,
    yr bigint,
    mnth bigint,
    holiday bigint,
    weekday bigint,
    workingday bigint,
    weathersit bigint,
    temp double precision,
    atemp double precision,
    hum double precision,
    windspeed double precision,
    casual bigint,
    registered bigint,
    cnt bigint
);

COPY reference FROM '/data/reference.csv' DELIMITER ',' CSV HEADER;

COMMIT;

ANALYZE reference;
ANALYZE current;