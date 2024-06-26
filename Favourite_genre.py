# -*- coding: utf-8 -*-

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from itertools import chain

def parse_user_input(line):
    fields = line.split('|')
    return Row(user_id=int(fields[0]), age=int(fields[1]), gender=fields[2], occupation=fields[3], zip=fields[4])

def parse_rating_input(line):
    fields = line.split('\t')
    return Row(user_id=int(fields[0]), item_id=int(fields[1]), rating=int(fields[2]), timestamp=int(fields[3]))

def parse_movie_input(line):
    fields = line.split('|')
    genres = fields[5:]
    return Row(item_id=int(fields[0]), title=fields[1], genres=[int(g) for g in genres])

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

    # Step 1: Filter users who have rated at least 50 movies
    active_users_df = spark.sql("""
        SELECT user_id, COUNT(item_id) as movie_count
        FROM ratings
        GROUP BY user_id
        HAVING movie_count >= 50
    """)
    active_users_df.createOrReplaceTempView("active_users")

    # Step 2: Join with ratings and movies to get the genres
    user_genres_df = spark.sql("""
        SELECT au.user_id, m.genres
        FROM active_users au
        JOIN ratings r ON au.user_id = r.user_id
        JOIN movies m ON r.item_id = m.item_id
    """)
    
    # Explode genres list to individual rows for counting
    user_genres_exploded_df = user_genres_df.withColumn("genre", F.explode(F.col("genres")))
    
    # Define genre mapping
    genre_mapping = {
        0: "Action",
        1: "Adventure",
        2: "Animation",
        3: "Children",
        4: "Comedy",
        5: "Crime",
        6: "Documentary",
        7: "Drama",
        8: "Fantasy",
        9: "Film-Noir",
        10: "Horror",
        11: "Musical",
        12: "Mystery",
        13: "Romance",
        14: "Sci-Fi",
        15: "Thriller",
        16: "War",
        17: "Western",
        18: "No genres listed"
    }

    # Map genre numbers to names
    genre_mapping_expr = F.create_map([F.lit(x) for x in chain(*genre_mapping.items())])
    user_genres_named_df = user_genres_exploded_df.withColumn("genre_name", genre_mapping_expr[F.col("genre")])
    
    # Step 3: Calculate favorite genres for each user
    favorite_genres_df = user_genres_named_df.groupBy("user_id", "genre_name").count()
    
    # Find the favorite genre by getting the max count for each user
    windowSpec = Window.partitionBy("user_id").orderBy(F.desc("count"))
    favorite_genres_ranked_df = favorite_genres_df.withColumn("rank", F.row_number().over(windowSpec))
    
    # Filter to get only the top-ranked (favorite) genres for each user
    favorite_genres_df = favorite_genres_ranked_df.filter(F.col("rank") == 1).drop("rank")
    
    favorite_genres_df.show(10)

    # Stop the session
    spark.stop()
