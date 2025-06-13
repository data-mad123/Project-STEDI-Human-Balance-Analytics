 CREATE TABLE IF NOT EXISTS step_trainer_landing (
         sensorReadingTime TIMESTAMP,
         serialNumber STRING,
         distanceFromObject FLOAT
     )
     ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
     LOCATION 's3://cd0091bucket/step_trainer/';
