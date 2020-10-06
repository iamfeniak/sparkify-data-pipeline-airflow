# Sparkify Data Pipeline with Airflow

## Introduction 

This project contains Data pipeline written in Airflow that extracts Sparkify data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to.

It also takes care of data quality checking, supports specific JSON format for the data, supports retries and failure handling. 

This is a learning project and not a real-world application. Use with caution. Sparkify does not exist.

## How to run it

### Pre-requisites
1. You must have configured Airflow to run this project. 
2. You must have added Redshift connection and AWS connection to perform the steps. 
3. Run script create_tables.py on your Redshift instance before starting the DAG to create necessary tables. 

### DAG
Here's how the execution graph looks like: 
![DAG](https://i.imgur.com/W1YkJw5.png)

### Running

DAG is scheduled to run hourly on Airflow. 
All the runs and possible issues can be observed in your Airflow instance. 

# License
Please refer to `LICENSE.md` to understand how to use this project for personal purposes.
