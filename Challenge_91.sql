-- set vars
set (wrk_role, wrk_db, wrk_wh) = ('RF_DCU_CORP_LEARNER', 'DCU_CORP_LEARNING', 'TEST');

-- for this exercice, $wrk_role must have create integration privilege
-- set query context
use role identifier($wrk_role);
use warehouse identifier($wrk_wh);
use database identifier($wrk_db);

-- create schema for the challenge (if db not exists create it)
create schema if not exists SNW_FROSTY_91;
use schema SNW_FROSTY_91;

-- create api integration dedicated for git
create api integration if not exists git_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/FrostyFridays/FF_week_91')
  ENABLED = TRUE;

-- create a link to the git repository
create git repository if not exists git_snw_frosty_91
  API_INTEGRATION = git_integration
  ORIGIN = 'https://github.com/FrostyFridays/FF_week_91';

-- do a fetch to update
alter git repository git_snw_frosty_91 fetch;

-- show branches
show git branches in git_snw_frosty_91;
  
-- browse the git repo as a stage
ls @git_snw_frosty_91/branches/main;

-- create the proc mapped to the snowpark source code stored on git
create or replace procedure proc_add_1(table_name VARCHAR, column_name VARCHAR)
    returns table()
    language python
    runtime_version='3.11'
    packages=('snowflake-snowpark-python')
    imports=('@git_snw_frosty_91/branches/main/add_1.py')
    handler='add_1.add_one_to_column';

-- create the sample table
create or replace table people as 
select * 
from 
    (
        VALUES 
            (1, 'Alice'),
            (2, 'Bob'),
            (3, 'Charlie'),
            (4, 'David'),
            (5, 'Eva'),
            (6, 'Fiona'),
            (7, 'George'),
            (8, 'Hannah'),
            (9, 'Ian'),
            (10, 'Julia')
    ) as t (id, name);

-- test the stored proc on it
call proc_add_1('PEOPLE','ID');

-- it works :)
/*
NAME	ID
Alice	2
Bob	    3
Charlie	4
David	5
Eva	    6
Fiona	7
George	8
Hannah	9
Ian	    10
Julia	11
*/
