---
title: "Load data into Snowflake" 
id: "3-connect-to-data-source"
description: "Load data into Snowflake"
---
We need to obtain our data source by copying our Formula 1 data into Snowflake tables from a public S3 bucket that dbt Labs hosts. 

1. Your new Snowflake account has a preconfigured warehouse named `COMPUTE_WH`. If for some reason you don’t have this warehouse, we can create a warehouse using the following script:

    ```sql
    create or replace warehouse COMPUTE_WH with warehouse_size=XSMALL
    ```
2. Rename the worksheet by clicking the worksheet name (this is automatically set to the current timestamp) using the option **…** and **Rename**. Rename the file  to `data setup script` since we will be placing code in this worksheet to ingest the Formula 1 data. Make sure your role is set as the **ACCOUNTADMIN** and select the **COMPUTE_WH** warehouse.

    <Lightbox src="/img/guides/dbt-ecosystem/dbt-python-snowpark/3-connect-to-data-source/1-rename-worksheet-and-select-warehouse.png" title="Rename worksheet and select warehouse"/>

3. Copy the following code into the main body of the Snowflake worksheet. You can also find this setup script under the `setup` folder in the [Git repository](https://github.com/dbt-labs/python-snowpark-formula1/blob/main/setup/setup_script_s3_to_snowflake.sql). The script is long since it's bringing in all of the data we'll need today! Generally during this lab we'll be explaining and breaking down the queries. We won't going line by line, but we will point out important information related to our learning objectives!

    ```sql
    -- create and define our formula1 database
    create or replace database formula1;
    use database formula1; 
    create or replace schema raw; 
    use schema raw; 

    -- define our file format for reading in the csvs 
    create or replace file format csvformat
    type = csv
    field_delimiter =','
    field_optionally_enclosed_by = '"', 
    skip_header=1; 

    --
    create or replace stage formula1_stage
    file_format = csvformat 
    url = 's3://formula1-dbt-cloud-python-demo/formula1-kaggle-data/';

    -- load in the 8 tables we need for our demo 
    -- we are first creating the table then copying our data in from s3
    -- think of this as an empty container or shell that we are then filling
    create or replace table formula1.raw.circuits (
        CIRCUITID NUMBER(38,0),
        CIRCUITREF VARCHAR(16777216),
        NAME VARCHAR(16777216),
        LOCATION VARCHAR(16777216),
        COUNTRY VARCHAR(16777216),
        LAT FLOAT,
        LNG FLOAT,
        ALT NUMBER(38,0),
        URL VARCHAR(16777216)
    );
    -- copy our data from public s3 bucket into our tables 
    copy into circuits 
    from @formula1_stage/circuits.csv
    on_error='continue';

    create or replace table formula1.raw.constructors (
        CONSTRUCTORID NUMBER(38,0),
        CONSTRUCTORREF VARCHAR(16777216),
        NAME VARCHAR(16777216),
        NATIONALITY VARCHAR(16777216),
        URL VARCHAR(16777216)
    );
    copy into constructors 
    from @formula1_stage/constructors.csv
    on_error='continue';

    create or replace table formula1.raw.drivers (
        DRIVERID NUMBER(38,0),
        DRIVERREF VARCHAR(16777216),
        NUMBER VARCHAR(16777216),
        CODE VARCHAR(16777216),
        FORENAME VARCHAR(16777216),
        SURNAME VARCHAR(16777216),
        DOB DATE,
        NATIONALITY VARCHAR(16777216),
        URL VARCHAR(16777216)
    );
    copy into drivers 
    from @formula1_stage/drivers.csv
    on_error='continue';

    create or replace table formula1.raw.lap_times (
        RACEID NUMBER(38,0),
        DRIVERID NUMBER(38,0),
        LAP NUMBER(38,0),
        POSITION FLOAT,
        TIME VARCHAR(16777216),
        MILLISECONDS NUMBER(38,0)
    );
    copy into lap_times 
    from @formula1_stage/lap_times.csv
    on_error='continue';

    create or replace table formula1.raw.pit_stops (
        RACEID NUMBER(38,0),
        DRIVERID NUMBER(38,0),
        STOP NUMBER(38,0),
        LAP NUMBER(38,0),
        TIME VARCHAR(16777216),
        DURATION VARCHAR(16777216),
        MILLISECONDS NUMBER(38,0)
    );
    copy into pit_stops 
    from @formula1_stage/pit_stops.csv
    on_error='continue';

    create or replace table formula1.raw.races (
        RACEID NUMBER(38,0),
        YEAR NUMBER(38,0),
        ROUND NUMBER(38,0),
        CIRCUITID NUMBER(38,0),
        NAME VARCHAR(16777216),
        DATE DATE,
        TIME VARCHAR(16777216),
        URL VARCHAR(16777216),
        FP1_DATE VARCHAR(16777216),
        FP1_TIME VARCHAR(16777216),
        FP2_DATE VARCHAR(16777216),
        FP2_TIME VARCHAR(16777216),
        FP3_DATE VARCHAR(16777216),
        FP3_TIME VARCHAR(16777216),
        QUALI_DATE VARCHAR(16777216),
        QUALI_TIME VARCHAR(16777216),
        SPRINT_DATE VARCHAR(16777216),
        SPRINT_TIME VARCHAR(16777216)
    );
    copy into races 
    from @formula1_stage/races.csv
    on_error='continue';

    create or replace table formula1.raw.results (
        RESULTID NUMBER(38,0),
        RACEID NUMBER(38,0),
        DRIVERID NUMBER(38,0),
        CONSTRUCTORID NUMBER(38,0),
        NUMBER NUMBER(38,0),
        GRID NUMBER(38,0),
        POSITION FLOAT,
        POSITIONTEXT VARCHAR(16777216),
        POSITIONORDER NUMBER(38,0),
        POINTS NUMBER(38,0),
        LAPS NUMBER(38,0),
        TIME VARCHAR(16777216),
        MILLISECONDS NUMBER(38,0),
        FASTESTLAP NUMBER(38,0),
        RANK NUMBER(38,0),
        FASTESTLAPTIME VARCHAR(16777216),
        FASTESTLAPSPEED FLOAT,
        STATUSID NUMBER(38,0)
    );
    copy into results 
    from @formula1_stage/results.csv
    on_error='continue';

    create or replace table formula1.raw.status (
        STATUSID NUMBER(38,0),
        STATUS VARCHAR(16777216)
    );
    copy into status 
    from @formula1_stage/status.csv
    on_error='continue';

    ```
4. Ensure all the commands are selected before running the query &mdash; an easy way to do this is to use Ctrl-A to highlight all of the code in the worksheet. Select **run** (blue triangle icon). Notice how the dot next to your **COMPUTE_WH** turns from gray to green as you run the query. The **status** table is the final table of all 8 tables loaded in. 

    <Lightbox src="/img/guides/dbt-ecosystem/dbt-python-snowpark/3-connect-to-data-source/2-load-data-from-s3.png" title="Load data from S3 bucket"/>

5. Let’s unpack that pretty long query we ran into component parts. We ran this query to load in our 8 Formula 1 tables from a public S3 bucket. To do this, we:
    - Created a new database called `formula1` and a schema called `raw` to place our raw (untransformed) data into. 
    - Created a stage to locate our data we are going to load in. Snowflake Stages are locations where data files are stored. Stages are used to both load and unload data to and from Snowflake locations. Here we are using an external stage, by referencing an S3 bucket. 
    - Created our tables for our data to be copied into. These are empty tables with the column name and data type. Think of this as creating an empty container that the data will then fill into. 
    - Used the `copy into` statement for each of our tables. We reference our staged location we created and upon loading errors continue to load in the rest of the data. You should not have data loading errors but if you do, those rows will be skipped and Snowflake will tell you which rows caused errors

6. Now let's take a look at some of our cool Formula 1 data we just loaded up!
    1. Create a new worksheet by selecting the **+** then **New Worksheet**.
        <Lightbox src="/img/guides/dbt-ecosystem/dbt-python-snowpark/3-connect-to-data-source/3-create-new-worksheet-to-query-data.png" title="Create new worksheet to query data"/>
    2. Navigate to **Database > Formula1 > RAW > Tables**. 
    3. Query the data using the following code. There are only 76 rows in the circuits table, so we don’t need to worry about limiting the amount of data we query.
        ```sql
        select * from formula1.raw.circuits
        ```
    4. Run the query. From here on out, we’ll use the keyboard shortcuts Command-Enter or Control-Enter to run queries and won’t explicitly call out this step. 
    5. Review the query results, you should see information about Formula 1 circuits, starting with Albert Park in Australia! 
    6. Finally, ensure you have all 8 tables starting with `CIRCUITS` and ending with `STATUS`. Now we are ready to connect into dbt Cloud!

        <Lightbox src="/img/guides/dbt-ecosystem/dbt-python-snowpark/3-connect-to-data-source/4-query-circuits-data.png" title="Query circuits data"/>

        