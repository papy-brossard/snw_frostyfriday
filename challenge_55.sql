-- set vars
set (wrk_role, wrk_db, wrk_wh) = ('RF_DCU_CORP_LEARNER', 'DCU_CORP_LEARNING', 'LOAD');

-- set query context
use role identifier($wrk_role);
use warehouse identifier($wrk_wh);
use database identifier($wrk_db);

-- create db schema if not exists
create schema if not exists SNW_FROSTY_55;
use schema SNW_FROSTY_55;

-- create stage where source files are stored
create stage if not exists PUBLIC.STG_FROSTY
    url='s3://frostyfridaychallenges/';

-- create file format to parse csv
create file format if not exists CSV_FORMAT1
    type = csv , 
    parse_header = true;

-- list files on stage
ls @PUBLIC.STG_FROSTY/challenge_55/;

-- test schema inferrence on files
select *
  from table(
    infer_schema(
      location=>'@PUBLIC.STG_FROSTY/challenge_55/',
      file_format=>'CSV_FORMAT1'
      )
    );

-- create table using schema inference and setting schema evolution
create or replace table W55_RAW_SALES
  using template (
    select array_agg(object_construct(*))
      from table(
        infer_schema(
          location=>'@PUBLIC.STG_FROSTY/challenge_55/',
		  file_format=>'CSV_FORMAT1'
        )
      )
  )
  with 
    enable_schema_evolution=true;

-- import data with schema evolution support
copy into W55_RAW_SALES 
	from @PUBLIC.STG_FROSTY/challenge_55/
  file_format = (
    format_name= 'CSV_FORMAT1'
  )
  match_by_column_name=case_insensitive;

-- remove dups
create table if not exists W55_CURATED_SALES as 
select *
from W55_RAW_SALES
group by all;

select count(1) from W55_RAW_SALES;
select count(1) from W55_CURATED_SALES;
