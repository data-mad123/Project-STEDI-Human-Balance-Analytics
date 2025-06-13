CREATE TABLE IF NOT EXISTS accelerometer_landing (
         timeStamp TIMESTAMP,
         user STRING,
         x FLOAT,
         y FLOAT,
         z FLOAT
     )
     ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
     LOCATION 's3://cd0091bucket/accelerometer/';
