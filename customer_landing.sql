CREATE TABLE IF NOT EXISTS customer_landing (
         serialnumber STRING,
         sharewithpublicasofdate DATE,
         birthday DATE,
         registrationdate DATE,
         sharewithresearchasofdate DATE,
         customername STRING,
         email STRING,
         lastupdatedate DATE,
         phone STRING,
         sharewithfriendsasofdate DATE
     )
     ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
     LOCATION 's3://cd0091bucket/customers/';
