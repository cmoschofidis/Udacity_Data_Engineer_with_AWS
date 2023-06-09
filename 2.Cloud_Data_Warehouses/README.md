# Project: Data Warehouse on AWS

## Introduction
This project aims to construct a Data Warehouse on AWS, extracting transactional data from S3 and load into analytical structure on Amazon Redshift.

## Project Datasets

### Song Dataset
The first dataset is a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/).
Each file is in JSON format and contains metadata about a song and the artist of that song.
The files are partitioned by the first three letters of each song's track ID. For example, here are file paths to two files in this dataset.
```
song_data/A/B/C/TRABCEI128F424C983.json
song_data/A/A/B/TRAABJL12903CDCF1A.json
```
And below is an example of what a single song file, TRAABJL12903CDCF1A.json, looks like.

```json
{
  "num_songs": 1, 
  "artist_id": "ARJIE2Y1187B994AB7", 
  "artist_latitude": null, 
  "artist_longitude": null, 
  "artist_location": "", 
  "artist_name": "Line Renaud", 
  "song_id": "SOUPIRU12A6D4FA1E1", 
  "title": "Der Kleine Dompfaff", 
  "duration": 152.92036, 
  "year": 0
}
```

### Log Dataset
The second dataset consists of log files in JSON format generated by this [event simulator](https://github.com/Interana/eventsim) based on the songs in the dataset above.
These simulate activity logs from a music streaming app based on specified configurations.

The log files in the dataset you'll be working with are partitioned by year and month. For example, here are filepaths to two files in this dataset.

```
log_data/2018/11/2018-11-12-events.json
log_data/2018/11/2018-11-13-events.json
```

## Database Schema
### Schema for Song Play Analysis
This project uses star schema with one fact table and four dimension tables to optimize ad-hoc queries on song play analysis.

#### Fact Table
1. **songplays** - records in log data associated with song plays i.e. records with page ```NextSong```
    - *songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent*
#### Dimension Tables
2. users - users in the app
    - *user_id, first_name, last_name, gender, level*
3. songs - songs in music database
    - *song_id, title, artist_id, year, duration*
4. artists - artists in music database
    - *artist_id, name, location, latitude, longitude*
5. time - timestamps of records in songplays broken down into specific units
    - *start_time, hour, day, week, month, year, weekday*

![](https://github.com/cmoschofidis/Udacity_Data_Engineer_with_AWS/blob/main/2.Cloud_Data_Warehouses/ERD.svg)

## Project Structure
Each task of the workflow which loads data from S3 buckets into analytical structure on AWS Redshift is detailed below:

**1. Create Resources:**
The first step to run the project is based on create all AWS resources and define permissions needed. This task can be performed directly through AWS Console however in this case it was decided to use a python script to manage the solution. 

**2. Create tables:**
With AWS enviroment set, the user needs to delete previous existing tables and create the staging and analytical structure. 

**3. ETL:**
The ETL proccess aims to populate staging tables with data from S3 and then process that data into analytics tables modeled following star-schema patterns.

**4. Delete Resources:**
Finally to avoid extra fees, the user needs to delete all the AWS resources created to run the workflow using the argument --delete on the python script that creates the resources.

## How to run
1. ```Modify dwh.cfg```
    - The user must insert information about AWS Project ID, AWS SECRET, AWS PERSONAL TOKEN
2. ```create_resources.py```
    - This script creates all the resources needed to run the workflow
3. ```create_tables.py```
    - Delete all tables if they exist and create new tables on a dimensional-model
4. ```etl.py```
    - Load data from S3 into staging tables on Amazon Redshift and process that data into analytics tables
6. ```create_resources.py --delete```
    - Delete all the resources created to run the workflow in order to prevent any extra fees from AWS.