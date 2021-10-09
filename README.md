# Assignment task for Data Engineer

## Introduction

Assume you are a Data Engineer working at upday. 

You are in close communication with our Business Intelligence team and you need to provide that data that they need for their daily duty.

At the same time, you have the necessary knowledge to find and fetch the raw data that upday receives from the app and make it ready for the BI team needs.

The scope of this task is build everything that is between raw data and BI tables.

## Assignment
The BI team in Upday requires 2 tables:
* article_performance: aggregating simple stats on how the article has performed
* user_performance: aggregating simple stats on how the user interacted with the app

<p align="center">
  <img src="https://upday-data-assignment.s3-eu-west-1.amazonaws.com/upday.jpeg" width="250" alt="Top news card">
</p>

The data is available at https://s3.console.aws.amazon.com/s3/buckets/upday-data-assignment/lake/ as tsv files
  * Each line in the files represent a collected event, the 1st line is the header to help interpret the schema
  * EVENT_NAME column represents the type of event collected 
    * top_news_card_viewed -> A card from Top news section has been viewed by the user
    * my_news_card_viewed -> A card from My news section has been viewed by the user
    * article_viewed -> The user clicked on the card and viewed the article in the app's web viewer
  * The Attributes column is the event payload as a JSON text
    * id = id of the article
    * category = category of the article
    * url
    * title 
    * etc...

The two tables should be created in the Postgres DB that is brought up by executing the docker-compose script in the project folder. They should look as the examples below:

<u>article_performance table</u>

| article_id  | date         | title           | category   | card_views | article_views |
|-------------|--------------|-----------------|------------|------------|---------------|
| id1         |  2020-01-01  | Happy New Year! |  Politics  |  1000      |    22         |

<u>user_performance table</u>

| user_id     | date         | ctr   |
|-------------|--------------|-------|
| id1         |  2020-01-01  |0.15   |

* ctr(click through rate) = number of articles viewed / number of cards viewed
* load the files directly from S3 instead of manually copying them locally 
* create staging tables as necessary for any intermediate steps in the process

## Instructions
You should make a pull request to this repository containing the solution. If you need to clarify any point of your solution, please include an explanation in the PR description.

What we expect:
* The code implementing the ETL logic, triggered from the `run.py` script
* Extension of the currently existing `Dockerfile` (only if needed)
* Tests, with instructions on how to run them

Some more details:
* The docker compose file will create a Postgres container already connected to the ETL container. You should use that Postgres instance for storing data models and solution.
* The task reviewer will only run `docker-compose up` and wait for the container to exit, 
* After the command prints `data-assignment-task_etl_1 exited with code 0`, the database is expected to be in its final state, with the solution to the task.

The task will be evaluated based on:
* Correctness of the final result
* Readability of the code
* Architecture of the solution
* Tests (full coverage is not required, you should mainly test the critical parts of the code)

To access the data in S3:
* Create new AWS account: https://aws.amazon.com/premiumsupport/knowledge-center/create-and-activate-aws-account/
* Access keys to programatically access S3 objects: https://docs.aws.amazon.com/powershell/latest/userguide/pstools-appendix-sign-up.html
* Access S3 objects from python: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html

