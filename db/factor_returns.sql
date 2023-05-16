CREATE TABLE factor_returns
(
    datadate            TIMESTAMP,
    factor              VARCHAR(100),
    timeframe           VARCHAR(20),
    mkt_cap_class       VARCHAR(20),
    top                 INTEGER,

    long_rtn            DECIMAL(25,15),
    short_rtn           DECIMAL(25,15),
    rtn                 DECIMAL(25,15),

    consistent          BOOLEAN,

    PRIMARY KEY (datadate, factor, timeframe, mkt_cap_class, top)
);