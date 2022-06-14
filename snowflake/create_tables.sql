USE BIGDATA
#######################################################################################################

CREATE TABLE IF NOT EXISTS air_polution( 
  record_date datetime,
  CO float, 
  NO FLOAT, 
  No2 FLOAT,
  o3 float,
  so2 float,
  pm2_5 float,
  pm10 float,
  nh3 float,
  air_quality int
);

CREATE TABLE IF NOT EXISTS air_polution_landing( 
  record_date datetime,
  CO float, 
  NO FLOAT, 
  No2 FLOAT,
  o3 float,
  so2 float,
  pm2_5 float,
  pm10 float,
  nh3 float,
  air_quality int
);
