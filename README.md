# Project - Airflow Data Pipeline for Sparkify

A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to orchestrate and query their data, which resides on AWS S3 in JSON logs on user activity on the app, as well as with JSON metadata on the songs in their app also on AWS S3.

This project creates a data pipeline using Airflow that orchestrates tasks to perform an ETL in AWS s3 and Redshift environment. In this project, a star schema was used where fact and dimension tables have been defined for a particular analytic focus. The ETL pipeline is used to load data from AWS S3 into Redshift for data extraction, then transformation is applied before loading the data into new tables in Redshift for analytic team to use.

Docker configs: Airflow is installed along with Postgres in using docker. the `docker-compose.yml` contains installation setup of the environment.

There 5 categories of tasks:
* Task to indicate the start and end of the ETL
* Tasks to first create tables in Redshift
* Task to load facts table in Redshift
* Tasks to load dimensions table in Redshift
* Task to check data quality and validation in Redshift

For the purposes of abstraction, custom operators have been create to optimize processing time. These can befound in the `operators` directory.

## Getting Started

The details below will get you a summary of the project's airflow data pipeline for development and testing purposes.

### Prerequisites

You need to create an AWS Redshift cluster and have the Amazon AIM credentials with access to s3 bucket in AWS. The Redshift connection and AWS connection will be setup using Airflow GUI configs. Information on where the data resides in AWS S3 is configured using the Airflow's module `Variable`. Before cloning the repo, make sure docker is installed on your system.

Airflow Variable accepts json file upload - See example below:
```
#Sparkify_configs.json
{
	"Sparkify_configs": {
		"s3_bucket": "",
		"s3_logdata_key": "",
		"s3_songdata_key": "",
		"LOG_JSONPATH": ""  #only the s3_datakey to jsonpath
	}
}
```

### Installation

Clone the project's repo in desired local directory. Create a `config.json` file inside `./dags/config/` and add information about datasets location in s3 like the above **Sparkify_configs.json**.

Open terminal in the main project's folder and run `docker-compose up -d --build` - this should build the `webser-server` image running at http://localhost:8080/ . navigate to this link and Airflow should be running with an `error`: `Variable Sparkify_configs does not exist` - this is expected.

Configuring Airflow:
* Click on Admin -> Variables, then drag your `config.json` file on "choose File". File will be uploaded, then click **"Import Variables"**.
* Click on Admin -> Connections, then choose "Create" from the menu to add aws credentials. Fill the form with `conn Id = aws_credentials_id`, `Conn Type = Amazon Web Services`, then `Login = your aws Access key ID`, and `Password = your aws Secret access key`. Then hit **"Save"**
* Click on Admin -> Connections, then choose "Create" from menu to add redshift connection. Fill the form with your redshift details. Note: for Redshift, choose  `conn Id = redshift` and `Conn Type = Postgres`. Then hit **"Save"**

If you are this far, congrats! :tada: The setup is done. Now you can run the dag :clap:

## Running the tests

Switch the `dag` to `ON` from the Airflow GUI. The tasks should start running as they are scheduled to every hour and have some backfills to perform. You can inspect the tasks using different Airflow's view such as "Tree View".

Once the Create table tasks are market complete, you can check in Redshift for the tables created. You can also run queries here to check for data loaded in tables.

The data quality check task should succeed if each table's data is valid. You can inspect the log for this task to see details more details.


## Authors

* **Adrien Ndikumana** - [adrien19](https://github.com/adrien19)


## Acknowledgments

* Project inspired by Udacity Team
