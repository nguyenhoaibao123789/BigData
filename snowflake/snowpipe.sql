CREATE OR REPLACE PIPE BIGDATA.PIPE.air_polution auto_ingest=true AS
  COPY INTO BIGDATA.PUBLIC.air_polution_landing 
  FROM (SELECT try_to_timestamp_ntz($1:"Datetime"::string),
        $1:"CO"::float,
        $1:"NO"::float,
        $1:"NO2"::float,
        $1:"O3"::float,
        $1:"SO2"::float,
        $1:"PM2_5"::float,
        $1:"PM10"::float,
        $1:"NH3"::float,
        $1:"Air_quality"::int
      FROM @BIGDATA.external_stages.json_folder);

DESCRIBE PIPE BIGDATA.PIPE.air_polution;

select * from table(information_schema.copy_history(table_name=>'BIGDATA.PUBLIC.AIR_POLUTION_LANDING',
                                                    start_time=>dateadd(hours,-1,current_timestamp())));