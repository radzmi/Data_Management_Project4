# Data_Management_Project4

This Project will tackle cassandra and spark2 simutaneously

19/6/2024
For today session, will start with reading and analyzing the question
From the question, i going to try using spark2 in zepline and run cassandra in putty kernel


Mindmap: MovieLens Analysis with Spark and Cassandra
Setup Environment

Install necessary libraries: pyspark, cassandra-driver
Download and extract MovieLens 100k Dataset
Initialize Sessions

Spark Session
Configure and start Spark session
Cassandra Session
Configure and start Cassandra session
Parse and Load Data

Parse u.user file
Read file
Parse lines
Create list of tuples
Convert to RDD and DataFrame
Create RDD from list
Convert RDD to DataFrame
Save DataFrame to HDFS (Parquet format)
Load and Create RDDs for Other Datasets

u.data
Read file
Parse lines to create RDD
Convert RDD to DataFrame
u.item
Read file
Parse lines to create RDD
Convert RDD to DataFrame (select necessary columns)
Write DataFrames to Cassandra Keyspace

Write Ratings DataFrame
Write to Cassandra table "ratings"
Write Movies DataFrame
Write to Cassandra table "movies"
Write Users DataFrame
Write to Cassandra table "users"
Read Tables from Cassandra

Read Ratings Table
Load from Cassandra into DataFrame
Read Movies Table
Load from Cassandra into DataFrame
Read Users Table
Load from Cassandra into DataFrame
Answer Questions

Calculate Average Rating for Each Movie
Group by movie ID
Aggregate average rating
Identify Top Ten Movies with Highest Average Ratings
Join average ratings with movies
Order by average rating (descending)
Limit to top 10
Find Users Who Rated at Least 50 Movies and Their Favourite Genres
-Group by user ID and count ratings
Filter users with at least 50 ratings
Join with users DataFrame


Hadoop seems lagging, so restart is needed and the analysis will be continued tomorrow
Find Users Younger than 20 Years Old
-Filter users DataFrame by age < 20
Find Users with Occupation "Scientist" Aged 30-40
-Filter users DataFrame by occupation and age range
