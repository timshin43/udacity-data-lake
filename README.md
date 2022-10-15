#### Database Description
The database warehouse_project for Sparkify is designed to keep track of what songs users play. The database is build as a star schema and consists of the following tables:
* songplays (Fact Table) - records associated with song plays, users and artists etc. 
* users (Dimension Table) - users in the app
* songs (Dimension Table) - songs in music database
* artists (Dimension Table) - artists in music database
* time (Dimension Table) - timestamps of records in songplays broken down into specific units



#### Files in the repo
- etl.py reads data from S3, processes that data using Spark, and writes them back to S3
- dl.cfgcontains  AWS credentials
- README.md provides discussion on a process and decisions


#### Pipeline and Schema info
- The project utilizes transactional data where each "song plays" event is a transaction. Thus, a snowflake schema design is chosen as the most approproate data model for this case
- The pipeline processes logs and song data to create 5 tables (1 fact, 5 dimensions) which are stored in S3 as parquete files. This file files can be easily retreived from S3 for further processing or laoding into databases. 
- Data is partioned to allow equal distribution and faster processing.