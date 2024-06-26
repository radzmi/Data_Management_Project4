# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F

def parse_user_input(line):
    fields = line.split('|')
    return Row(user_id=int(fields[0]), age=int(fields[1]), gender=fields[2], occupation=fields[3], zip=fields[4])

def parse_rating_input(line):
    fields = line.split('\t')
    return Row(user_id=int(fields[0]), item_id=int(fields[1]), rating=int(fields[2]), timestamp=int(fields[3]))

def parse_movie_input(line):
    fields = line.split('|')
    genres = fields[5:]
    return Row(item_id=int(fields[0]), title=fields[1], genres=genres)

if __name__ == "__main__":
    # Create a SparkSession
    spark = SparkSession.builder \
        .appName("CassandraIntegration") \
        .config("spark.cassandra.connection.host", "127.0.0.1") \
        .getOrCreate()

    # Load user data
    user_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/radzmishah/ml-100k/u.user")
    users = user_lines.map(parse_user_input)
    users_df = spark.createDataFrame(users)

    # Write user data into Cassandra
    users_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="users", keyspace="movielens") \
        .save()

    # Load rating data
    rating_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/radzmishah/ml-100k/u.data")
    ratings = rating_lines.map(parse_rating_input)
    ratings_df = spark.createDataFrame(ratings)

    # Write rating data into Cassandra
    ratings_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="ratings", keyspace="movielens") \
        .save()

    # Load movie data
    movie_lines = spark.sparkContext.textFile("hdfs:///user/maria_dev/radzmishah/ml-100k/u.item")
    movies = movie_lines.map(parse_movie_input)
    movies_df = spark.createDataFrame(movies)

    # Write movie data into Cassandra
    movies_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .mode('append') \
        .options(table="movies", keyspace="movielens") \
        .save()
		
    # Read data back from Cassandra
    users_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="users", keyspace="movielens") \
        .load()

    ratings_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="ratings", keyspace="movielens") \
        .load()

    movies_df = spark.read \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="movies", keyspace="movielens") \
        .load()

    # Create temporary views for SQL queries
    users_df.createOrReplaceTempView("users")
    ratings_df.createOrReplaceTempView("ratings")
    movies_df.createOrReplaceTempView("movies")

    # i) Calculate the average rating for each movie
    avg_ratings_df = spark.sql("""
        SELECT item_id, AVG(rating) as avg_rating
        FROM ratings
        GROUP BY item_id
    """)
    avg_ratings_df.createOrReplaceTempView("avg_ratings")
    avg_ratings_df.show(10)

    # ii) Identify the top ten movies with the highest average ratings
    top_movies_df = spark.sql("""
        SELECT m.title, ar.avg_rating
        FROM avg_ratings ar
        JOIN movies m ON ar.item_id = m.item_id
        ORDER BY ar.avg_rating DESC
        LIMIT 10
    """)
    top_movies_df.show()

    # iv) Find all the users with age that is less than 20 years old
    young_users_df = spark.sql("SELECT * FROM users WHERE age < 20")
    young_users_df.show(10)
	
	# v) Find all the users who have the occupation “scientist” and their age is between 30 and 40 years old
    scientists_df = spark.sql("SELECT * FROM users WHERE occupation = 'scientist' AND age BETWEEN 30 AND 40")
    scientists_df.show(10)

    # Stop the session
    spark.stop()

