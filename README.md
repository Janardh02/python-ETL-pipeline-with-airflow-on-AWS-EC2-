ETL Pipeline Project with Apache Airflow

Overview:

This project demonstrates how to build and automate an ETL process using Apache Airflow to extract, transform, and load weather data. The data is sourced from the OpenWeatherMap API, transformed to meet analytical requirements, and stored in an AWS S3 bucket.

The project is carried out entirely on the AWS Cloud Platform, leveraging Apache Airflow's capabilities for orchestrating and scheduling workflows.
Project Objectives

    Extract: Fetch current weather data from the OpenWeatherMap API.
    Transform: Process and clean the data for better usability.
    Load: Store the processed data in an AWS S3 bucket.

Key Features

    Apache Airflow Setup: Step-by-step installation and configuration of Apache Airflow from scratch.
    DAG Design: Create Directed Acyclic Graphs (DAGs) to orchestrate ETL workflows.
    Operators & Sensors:
        Utilize Airflow operators to manage tasks in the ETL pipeline.
        Implement sensors to monitor data availability and trigger workflows accordingly.
    AWS Integration: Store processed data securely in an AWS S3 bucket.

Learning Outcomes

    Understand fundamental concepts of Apache Airflow such as DAGs and Operators.
    Learn how to set up and configure Airflow in a cloud environment.
    Gain hands-on experience in building, orchestrating, and scheduling an ETL pipeline.
