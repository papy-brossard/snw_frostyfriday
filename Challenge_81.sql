-- set vars
set (wrk_role, wrk_db, wrk_wh) =('RF_DCU_CORP_LEARNER', 'DCU_CORP_LEARNING', 'LOAD');

-- set query context
use role identifier($wrk_role);
use warehouse identifier($wrk_wh);
use database identifier($wrk_db);

-- create stage where source files are stored
create stage if not exists PUBLIC.STG_FROSTY
    url='s3://frostyfridaychallenges/';
    
-- list files on stage
ls @PUBLIC.STG_FROSTY/challenge_81/;

-- execute sql file stored onto stage
execute immediate from @PUBLIC.STG_FROSTY/challenge_81/starter_code.sql;

-- renaming schema in dcube learning context and use it
drop schema if exists SNW_FROSTY_81;
alter schema W81_RAW rename to SNW_FROSTY_81;
use schema SNW_FROSTY_81;

-- list objects created from the sql script on stage
show objects;

-- create stored proc to load raw data in truncate/insert mode
create or replace procedure proc_load_raw()
returns integer
as
  declare
    result integer default 0;
  begin
    truncate table w81_raw_product;
    insert into w81_raw_product (data)
        select
            parse_json(column1)
        from
            values
                ('{"product_id": 21, "product_name": "Product U", "category": "Electronics", "price": 120.99, "created_at": "2024-02-16"}'),
                ('{"product_id": 22, "product_name": "Product V", "category": "Books", "price": 35.00, "created_at": "2024-02-16"}');
        
    truncate table w81_raw_customer;
    insert into w81_raw_customer (data)
        select parse_json(column1)
        from
            values
                ('{"customer_id": 6, "customer_name": "Frank", "email": "frank@example.com", "created_at": "2024-02-16"}'),
                ('{"customer_id": 7, "customer_name": "Grace", "email": "grace@example.com", "created_at": "2024-02-16"}');

    truncate table w81_raw_sales;
    insert into w81_raw_sales (data)
        select parse_json(column1)
        from
            values
                ('{"sale_id": 11, "product_id": 21, "customer_id": 6, "quantity": 1, "sale_date": "2024-02-17"}'), 
                ('{"sale_id": 12, "product_id": 22, "customer_id": 1, "quantity": 1, "sale_date": "2024-02-17"}'),
                ('{"sale_id": 13, "product_id": 2, "customer_id": 7, "quantity": 2, "sale_date": "2024-02-17"}'),
                ('{"sale_id": 14, "product_id": 3, "customer_id": 6, "quantity": 1, "sale_date": "2024-02-17"}'),
                ('{"sale_id": 15, "product_id": 21, "customer_id": 5, "quantity": 1, "sale_date": "2024-02-17"}');
    
    return result;
  end;

-- create a task to execute the stored proc to load raw data
create or replace task TASK_LOAD_RAW
warehouse=LOAD
as
call proc_load_raw();

-- create stored proc to refine customer data
create or replace procedure proc_refine_customer()
returns integer
as
  declare
    result integer default 0;
  begin
    create or replace table W81_REFINED_CUSTOMER as 
        select 
            DATA:"CREATE OR REPLACEd_at"::date as ldts,
            DATA:"customer_id"::number as customer_id,
            DATA:"customer_name"::string as customer_name,
            DATA:"email"::string as email
        from W81_RAW_CUSTOMER;
    return result;
  end;

-- create a task to execute the stored proc to refine customer raw data
create or replace task task_refine_customer
warehouse=TRANSFORM
after task_load_raw
as   
call proc_refine_customer();

-- create stored proc to refine product data
create or replace procedure proc_refine_product()
returns integer
as
  declare
    result integer default 0;
  begin
    create or replace table W81_REFINED_PRODUCT as 
        select 
            DATA:"CREATE OR REPLACEd_at"::date as ldts,
            DATA:"category"::string as category,
            DATA:"price"::float as price,
            DATA:"product_id"::number as product_id,
            DATA:"product_name"::string as product_name
        from W81_RAW_PRODUCT;
    return result;
  end;

-- create a task to execute the stored proc to refine customer raw data
create or replace task task_refine_product
warehouse=TRANSFORM
after task_load_raw
as   
call proc_refine_product();

-- create stored proc to refine product data
create or replace procedure proc_refine_sales()
returns integer
as
  declare
    result integer default 0;
  begin
    create or replace table W81_REFINED_SALES as 
        select 
            DATA:"customer_id"::number as customer_id,
            DATA:"product_id"::number as product_id,
            DATA:"quantity"::number as quantity,
            DATA:"sale_date"::date as sale_date,
            DATA:"sale_id"::number as sale_id
        from W81_RAW_SALES;
    return result;
  end;

-- create a task to execute the stored proc to refine sales raw data
create or replace task task_refine_sales
warehouse=TRANSFORM
after task_load_raw
as   
call proc_refine_sales();

-- create a task to aggregate sales refined data
create or replace task task_aggregate_sales
warehouse=TRANSFORM
after task_refine_customer, task_refine_product, task_refine_sales
as    
create or replace view AGGREGATED_SALES
as
select 
    c.customer_name,
    p.product_name,
    sum(s.quantity) as total_quantity,
    sum(s.quantity * p.price) as total_sales
from 
    w81_refined_sales s
    join w81_refined_product p on s.product_id = p.product_id
    join w81_refined_customer c on s.customer_id = c.customer_id
group by all
;

-- show the dag 
select
    t.name,
    t.predecessors,
    t.state
from 
    table(
        information_schema.Task_dependents(task_name => 'TASK_LOAD_RAW', recursive => TRUE)
        ) t;

-- resume tasks
alter task if exists TASK_AGGREGATE_SALES resume;
alter task if exists TASK_REFINE_CUSTOMER resume;
alter task if exists TASK_REFINE_PRODUCT resume;
alter task if exists TASK_REFINE_SALES resume;

-- execute the all dag
execute task TASK_LOAD_RAW;

