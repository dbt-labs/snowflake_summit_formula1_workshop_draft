---
title: "Leverage dbt Cloud to generate analytics and ML-ready pipelines with SQL and Python with Snowflake" 
id: "1-overview-dbt-python-snowpark"
description: "Leverage dbt Cloud to generate analytics and ML-ready pipelines with SQL and Python with Snowflake"
---
The focus of this workshop will be to demonstrate how we can use both *SQL and python together* in the same workflow to run *both analytics and machine learning models* on dbt Cloud.

All code in today’s workshop can be found on [GitHub](https://github.com/dbt-labs/python-snowpark-formula1/tree/python-formula1).

## What you'll use during the lab

- A [Snowflake account](https://trial.snowflake.com/) with ACCOUNTADMIN access
- A [dbt Cloud account](https://www.getdbt.com/signup/)

## What you'll learn

- How to use dbt with Snowflake to build scalable transformations using SQL and Python
    - How to use dbt SQL to prepare your data from sources to encoding 
    - How to train a model in dbt python and use it for future prediction 
    - How to deploy your full project 

## What you need to know

- Basic to intermediate SQL and python.
- Basic understanding of dbt fundamentals. We recommend the [dbt Fundamentals course](https://courses.getdbt.com/collections) if you're interested.
- High level machine learning process (encoding, training, testing)
- Simple ML algorithms &mdash; we will use logistic regression to keep the focus on the *workflow*, not algorithms!

## What you'll build

- A set of data analytics and prediction pipelines using Formula 1 data leveraging dbt and Snowflake, making use of best practices like data quality tests and code promotion between environments
- We will create insights for:
    1. Finding the lap time average and rolling average through the years (is it generally trending up or down)?
    2. Which constructor has the fastest pit stops in 2021?
    3. Predicting the position of each driver based on a decade of data. 

As inputs, we are going to leverage Formula 1 datasets hosted on a dbt Labs public S3 bucket. We will create a Snowflake Stage for our CSV files then use Snowflake’s `COPY INTO` function to copy the data in from our CSV files into tables. The Formula 1 is available on [Kaggle](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020). The data is originally compiled from the [Ergast Developer API](http://ergast.com/mrd/).