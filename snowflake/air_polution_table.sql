truncate table air_polution;
truncate table air_polution_landing;

create or replace stream air_polution_landing_stream on table air_polution_landing;
select * from air_polution_landing_stream;

create or replace task stream_to_main_table
warehouse=compute_wh
schedule='3 minute'  
when
system$stream_has_data('air_polution_landing_stream')
as
merge into air_polution main
using (select * from air_polution_landing_stream) landing_stream
on main.record_date = landing_stream.record_date 
when not matched 
then
insert(record_date,
  CO, 
  NO, 
  No2,
  o3,
  so2,
  pm2_5,
  pm10,
  nh3,
  air_quality)
values (landing_stream.record_date,
  landing_stream.CO, 
  landing_stream.NO, 
  landing_stream.No2,
  landing_stream.o3,
  landing_stream.so2,
  landing_stream.pm2_5,
  landing_stream.pm10,
  landing_stream.nh3,
  landing_stream.air_quality);

Create or replace task src_file_remove
warehouse=compute_wh
after stream_to_main_table
as
remove '@BIGDATA.external_stages.json_folder';

Create or replace task truncate_table
warehouse=compute_wh
after src_file_remove
as
truncate table air_polution_landing;

SHOW TASKS;
alter task TRUNCATE_TABLE resume;
alter task SRC_FILE_REMOVE resume;
alter task STREAM_TO_MAIN_TABLE resume;

alter task STREAM_TO_MAIN_TABLE suspend;
alter task SRC_FILE_REMOVE suspend;
alter task TRUNCATE_TABLE suspend;

select * from table(information_schema.task_history()); 

select * from air_polution
order by record_date