use role sysadmin;
drop database if exists data_project;
create database data_project;

create schema dev;

create or replace table data_project.dev.src_video (
    DateTime varchar(50),
    VideoTitle varchar(200),
    events varchar(200)
);

--create file format
create or replace file format data_project.dev.format_src_video
type='csv'
field_delimiter = ','
skip_header =1
field_optionally_enclosed_by='"';

--create an external s3 stage:
create or replace stage data_project.DEV.stage_src_video
url='s3://Bucket_folder/'
credentials=(AWS_KEY_ID='XXXXXXXXXXX' AWS_SECRET_KEY='XXXXXXXXXXXXXXXXX')
;

--verify the stage is created:
show stages;

--create a pipe using auto_ingest=true
create or replace pipe data_project.dev.pipe_src_video auto_ingest=true
as copy into data_project.dev.src_video
from @data_project.dev.stage_src_video
file_format = (format_name = data_project.dev.format_src_video)
on_error = 'skip_file';

--verify the pipe is created using:
show pipes;

--check if the uploaded file is in stage location
ls @data_project.dev.stage_src_video;

--check pipe status
select system$pipe_status('data_project.dev.pipe_src_video');

--check the copy history
select *
from table(information_schema.copy_history(TABLE_NAME=>'src_video', START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP())));

--Validate any loads for the pipe within the previous hour:
select * from table(validate_pipe_load(
  pipe_name=>'data_project.dev.pipe_src_video',
  start_time=>dateadd(hour, -1, current_timestamp())));

///*
//---The below auditing and validation is done for setting up and testing data pipeline
//---Data auditing-----
//select max(length(datetime)), max(length(videotitle)), max(length(events)) from src_video;
//
//---validate the data----
//select count(*) from data_project.dev.src_video
//where contains(events,'206')
//union all
//select count(*) from data_project.dev.src_video
//where events like '%206%'
//union all
//select count(*) from data_project.dev.src_video
//;
//
//select * from data_project.dev.src_video
//where videotitle like '%\\%';
//
//select * from data_project.dev.src_video
//where ARRAY_SIZE(split(videotitle,'|')) = 1;
//
//select distinct split(videotitle,'|')[0] as platform
//from data_project.dev.src_video;
//
//
//select distinct SPLIT_PART(videotitle,'|',-1)as video_title
//from data_project.dev.src_video;
//
//
//select datetime::timestamp as Date_Time, minute(Date_Time) from data_project.dev.src_video limit 100;
//---------------------The above auditing and validation is done for setting up and testing data pipeline 
//*/

----create scream to watch change on source table
CREATE OR REPLACE STREAM data_project.dev.stream_src_video ON TABLE data_project.dev.src_video;



-----create a view to save the transformed data
DROP VIEW If Exists data_project.dev.vw_src_video;

-- Create stream to watch on source table
create or replace stream data_project.dev.stream_src_credit on table data_project.dev.src_video; 
                      

-- create task to parse source data
--  only keep video_events contain 206 and discard title.split('|').count=1
CREATE OR REPLACE task DATALAKE_DEMO.DEV.task_video 
  warehouse = COMPUTE_WH
  schedule = '60 minute' 
when system$stream_has_data('data_project.dev.stream_src_credit')
AS
(
  CREATE OR REPLACE VIEW data_project.dev.vw_src_video AS
  (
    select 
        DateTime::timestamp as Date_Time,
        year(Date_Time) as Year,
        quarter(Date_time) as Quarter,
        month(Date_Time) as Month,
        week(Date_Time) as Week,
        dayofweek(Date_Time) as Day_of_week,
        hour(Date_Time) as Hour,
        minute(Date_Time) as MINUTE,
        case when lower(trim(split(videotitle,'|')[0]) like '%iphone%' then 'iPhone'
             when lower(trim(split(videotitle,'|')[0]) like '%android%' then 'Android Phone'
             when lower(trim(split(videotitle,'|')[0]) like '%ipad%' then 'iPad'
        else 'Desktop'
        end as platform,  
        initcap(replace(split(videotitle,'|')[0],'""','')) as site, 
        SPLIT_PART(videotitle,'|',-1) as video,
        events
    from data_project.dev.src_video
    where events like '%206%' and ARRAY_SIZE(split(videotitle,'|')) >1
    )
);
  
  ---check the view
  Select * from data_project.dev.vw_src_video limit 1000; 
  Select count(distinct DATETIME) from data_project.dev.src_video
  union
  Select count(DATETIME) from data_project.dev.src_video;
  
  
  ---create dimension table DimPlatform
--DROP TABLE data_project.DEV.DimPlatform;

CREATE OR REPLACE TABLE data_project.dev.DimPlatform
(
  Platform_Skey number AUTOINCREMENT PRIMARY KEY,
  Platform VARCHAR2(200 BYTE) not null
);
----populate DimPlatform
merge into data_project.dev.DimPlatform dest
  using (select distinct Platform from data_project.dev.vw_src_video) src
  on src.Platform = dest.Platform
  when not matched
  then insert (
      Platform)
      values (
          src.Platform);
          
---create dimension table DimVideo
DROP TABLE data_project.dev.DimVideo;

CREATE OR REPLACE TABLE data_project.dev.DimVideo
(
  Video_Skey number AUTOINCREMENT PRIMARY KEY,
  video VARCHAR2(200 BYTE) not null
);

merge into data_project.dev.DimVideo dest
  using (select distinct video from data_project.dev.vw_src_video) src
  on src.video = dest.video
  when not matched
  then insert (
      video)
      values (
          src.video);

---create dimension table DimSite

--The DROP TABLE IF EXISTS data_project.DEV.DimSite;

CREATE OR REPLACE TABLE data_project.DEV.DimSite
(
  Site_Skey number AUTOINCREMENT PRIMARY KEY,
  site VARCHAR2(200 BYTE) not null
);

merge into data_project.dev.DimSite dest
  using (SELECT DISTINCT site FROM data_project.dev.vw_src_video) src
  on src.site = dest.site
  when not matched
  then insert (
      site)
      values (
          src.site);
          
---create dimension table DimDate
CREATE OR REPLACE TABLE data_project.DEV.DimDate
(
  DateTime_Skey DATETIME primary key,
  Year int Not null,
  Quarter int Not null,
  Month int not null,
  week int not null,
  Day_of_week int not null,
  hour int not null,
  minute int not null
);

merge into data_project.dev.DimDate dest
  using (SELECT DISTINCT Date_Time::timestamp_ntz as DateTime_Skey,Year, Quarter,Month,week,Day_of_week,hour,minute FROM data_project.dev.vw_src_video) src
  on src.DateTime_Skey = dest.DateTime_Skey
  when not matched
  then insert (
      DateTime_Skey,Year, Quarter,Month,week,Day_of_week,hour,minute)
      values (
          src.DateTime_Skey, src.Year, src.Quarter, src.Month, src.week, src.day_of_week, src.hour, src.minute);
          
----create fact table VideoStart_fact
drop table if exists data_project.dev.VideoStart_fact;
                    
Create or replace table data_project.dev.VideoStart_fact(
    DateTime_Skey DATETIME not null,
    platform_Skey number not null,
    video_Skey number not null,
    site_Skey number not null,
    events text not null,
    PRIMARY KEY (DateTime_Skey,platform_Skey, video_Skey, site_Skey)
    );



merge into data_project.dev.VideoStart_fact dest
    using (
        select D.DateTime_Skey, P.platform_Skey, V.video_Skey, S.site_Skey, M.events from  data_project.dev.vw_src_video as M
        left join data_project.dev.dimdate as D on D.DateTime_Skey = M.Date_Time::timestamp_ntz
        left join data_project.dev.dimplatform as P on P.PLATFORM = M.PLATFORM
        left join data_project.dev.dimvideo as V on V.Video = M.video
        left join data_project.dev.dimSite as S on S.site = M.site
    ) src
    on src.platform_Skey = dest.platform_Skey
    and src.DateTime_Skey = dest.DateTime_Skey
    and src.video_Skey = dest.video_Skey
    and src.site_Skey = dest.site_Skey
    and src.events = dest.events
     when not matched
      then insert (
          platform_Skey,DateTime_Skey, video_Skey,site_Skey,events)
          values (
              src.platform_Skey, src.DateTime_Skey, src.video_Skey, src.site_Skey, src.events);
              
 --select count(*) from data_project.dev.vw_src_video;
    
