CREATE TABLE factor_loader_config
(
    factor              VARCHAR(100),
    timeframe           VARCHAR(20),

    last_date_persisted TIMESTAMP,
    source_table        VARCHAR(20),

    PRIMARY KEY (factor, timeframe)
);