#!/usr/bin/env python
# coding: utf-8

# # Question 1

# ## Initiate Spark

# In[1]:


from pyspark.sql import SparkSession
#from sparkmeasure import StageMetrics
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create a new Spark Session
spark = SparkSession        .builder        .master("spark://ubuntu:7077")        .appName("movies")        .getOrCreate()

# Create spark metrics object
#stagemetrics = StageMetrics(spark)


# In[2]:


#spark


# ## Load Datasets To Dataframes

# ### Movie Dataframe

# In[3]:


movie_df = (spark
           .read
           .format("csv")
           .option("header", "true")
           .option("delimiter", ",")
           .option("inferSchema", "true")
           .load("datasets/movie.csv")
           )


# In[4]:


#movie_df.printSchema()


# In[5]:


#movie_df.show(5)


# ### Rating Dataframe

# In[6]:


rating_df = (spark
            .read
            .format("csv")
            .option("header", "true")
            .option("delimiter", ",")
            .option("inferSchema", "true")
            .load("datasets/rating.csv")
            )


# In[7]:


#rating_df.printSchema()


# In[8]:


#rating_df.show(5)


# ### Tag Dataframe

# In[9]:


tag_df = (spark
         .read
         .format("csv")
         .option("header", "true")
         .option("delimiter", ",")
         .option("inferSchema", "true") 
         .load("datasets/tag.csv")
         )


# In[10]:


#tag_df.printSchema()


# In[11]:


#tag_df.show(5)


# ## Queries

# ### Query 1

# In[12]:


# Start measuring performance
stagemetrics.begin()

# Get the id of the movie "Jumanji"
jumanji_id = movie_df             .filter(movie_df.title.contains("Jumanji"))             .select("movieId")             .collect()[0]["movieId"]

# Get the number of users that watched "Jumanji"
query_1_result = rating_df                 .filter(rating_df["movieId"] == jumanji_id)                 .select(count("movieId"))

# Show result
query_1_result.show()

# Stop measuring performance
stagemetrics.end()

# Print performance metrics
print(stagemetrics.report().split('\n')[6])


# ### Query 2

# In[13]:


# Start measuring performance
stagemetrics.begin()

# Get the movieIds with tags containing the word "boring"
unique_boring_movieIds = tag_df                         .filter(lower(tag_df["tag"]).contains("boring"))                         .select("movieId")                         .dropDuplicates()

# Get the corresponding movie titles from movieIds in alphabetical order
query_2_results = unique_boring_movieIds                  .join(movie_df, "movieId", "inner")                  .select(movie_df.title)                  .sort(movie_df.title)

# Show results
query_2_results.show()

# Stop measuring performance
stagemetrics.end()

# Print performance metrics
print(stagemetrics.report().split('\n')[6])


# ### Query 3

# In[14]:


# Start measuring performance
stagemetrics.begin()

# Get the userIds and movieIds with tags containing the word "bollywood"
bollywood_userIds_movieIds = tag_df                             .filter(lower(tag_df["tag"]).contains("bollywood"))                             .select(["userId", "movieId", "tag"])

# Get all userIds and movieIds with rating above 3
above_3_rating = rating_df                 .filter(rating_df.rating > 3)                 .select(["userId", "movieId", "rating"])

# Inner join based on unique combination of userId and movieId
query_3_results = bollywood_userIds_movieIds                  .join(above_3_rating, ["userId", "movieId"], "inner")                  .select("userId")                  .dropDuplicates()                  .sort(above_3_rating.userId)

# Show results
query_3_results.show()

# Stop measuring performance
stagemetrics.end()

# Print performance metrics
print(stagemetrics.report().split('\n')[6])


#  ### Query 4

# In[15]:


# Start measuring performance
stagemetrics.begin()

# Group by year and movieId and find the mean value of rating
grouped_data = rating_df               .withColumn("year", year(rating_df["timestamp"]))               .groupBy(["year", "movieId"])               .agg({"rating": "mean"})
              
# Create a window to limit ratings
window = Window         .partitionBy(grouped_data["year"])         .orderBy(grouped_data["avg(rating)"]         .desc())

# Limit the ratings in each group
grouped_data_limited = grouped_data                       .select('*', row_number().over(window).alias('row_number'))                       .filter(col('row_number') <= 10)                       .sort(["year", "avg(rating)"], ascending = [True, False])

# Get the movie titles
query_4_results = grouped_data_limited                  .join(movie_df, "movieId", "inner")                  .select(["year", "title", "avg(rating)"])                  .withColumnRenamed("avg(rating)", "average_rating")

# Show results
query_4_results.show()

# Stop measuring performance
stagemetrics.end()

# Print performance metrics
print(stagemetrics.report().split('\n')[6])


# ### Query 5

# In[16]:


# Start measuring performance
stagemetrics.begin()

# Get the id and title of 2015 movies
movies_2015 = movie_df              .filter(movie_df.title.contains("(2015)"))              .select(["movieId", "title"])

# Get the tags of 2015 movies
joined = movies_2015         .join(tag_df, "movieId", "inner")         .select(["title", "tag"])         .sort("title")

# Group tags by movie title and concatenate them
query_5_results = joined                  .groupby("title")                  .agg(concat_ws(", ", collect_list(joined.tag)).alias("tags"))

# Show results
query_5_results.show()

# Stop measuring performance
stagemetrics.end()

# Print performance metrics
print(stagemetrics.report().split('\n')[6])


# ### Query 6

# In[17]:


# Start measuring performance
stagemetrics.begin()

# Group rating by movieId and get movie title from inner join
query_6_results = rating_df                  .groupBy("movieId")                  .agg({"rating": "count"})                  .join(movie_df, "movieId", "inner")                  .select(["title", "count(rating)"])                  .sort("count(rating)", ascending=False)                  .withColumnRenamed("count(rating)", "total_ratings")

# Show results
query_6_results.show()

# Stop measuring performance
stagemetrics.end()

# Print performance metrics
print(stagemetrics.report().split('\n')[6])


# ### Query 7

# In[18]:


# Start measuring performance
stagemetrics.begin()

# Group by year and userId and count the ratings for each group
grouped_data = rating_df               .withColumn("year", year(rating_df["timestamp"]))               .groupBy(["year", "userId"])               .agg({"rating": "count"})

# Create a window to limit ratings
window = Window         .partitionBy(grouped_data["year"])         .orderBy(grouped_data["count(rating)"]         .desc())

# Limit the ratings in each group
query_7_results = grouped_data                  .select('*', row_number().over(window).alias('row_number'))                  .filter(col('row_number') <= 10)                  .select(["year", "userId", "count(rating)"])                  .sort(["year", "count(rating)"], ascending = [True, False])                  .withColumnRenamed("count(rating)", "total_ratings")

# Show results
query_7_results.show()

# Stop measuring performance
stagemetrics.end()

# Print performance metrics
print(stagemetrics.report().split('\n')[6])


# ### Query 8

# In[19]:


# Start measuring performance
stagemetrics.begin()

# Keep the first genre of each movie
first_genre = movie_df              .select('*', split(movie_df.genres, '[|]')[0].alias("genre"))              .drop("genres")

# Get total ratings by movie
total_ratings_by_movie = first_genre                         .join(rating_df, "movieId", "inner")                         .groupBy(["genre", "title"])                         .agg({"*": "count"})                         .withColumnRenamed("count(1)", "total_ratings")

# Create a window to limit total ratings
window = Window         .partitionBy(total_ratings_by_movie["genre"])         .orderBy(total_ratings_by_movie["total_ratings"]         .desc())

# Limit the total ratings in each group
query_8_results = total_ratings_by_movie                  .select('*', row_number().over(window).alias('row_number'))                  .filter(col('row_number') <= 1)                  .select(["genre", "title", "total_ratings"])                  .sort(["genre"])                  .filter(total_ratings_by_movie.genre != "(no genres listed)")

# Show results
query_8_results.show()

# Stop measuring performance
stagemetrics.end()

# Print performance metrics
print(stagemetrics.report().split('\n')[6])


# ### Query 9

# In[20]:


# Start measuring performance
stagemetrics.begin()

# Group by year, month, day of month, hour and movieId and find the concurrent viewers for each movie
grouped_data = rating_df               .withColumn("year", year(rating_df["timestamp"]))               .withColumn("month", month(rating_df["timestamp"]))               .withColumn("dayofmonth", dayofmonth(rating_df["timestamp"]))               .withColumn("hour", hour(rating_df["timestamp"]))               .groupBy(["movieId","year", "month", "dayofmonth", "hour"])               .agg({"*": "count"})               .withColumnRenamed("count(1)", "concurrent_viewers")               .filter(col("concurrent_viewers") > 1)

# Get the sum of concurrent viewers
query_9_result = grouped_data.select(sum("concurrent_viewers"))

# Show result
query_9_result.show()

# Stop measuring performance
stagemetrics.end()

# Print performance metrics
print(stagemetrics.report().split('\n')[6])


# ### Query 10

# In[21]:


# Start measuring performance
stagemetrics.begin()

# Keep the first genre of each movie
first_genre = movie_df              .select('*', split(movie_df.genres, '[|]')[0].alias("genre"))              .drop("genres")

# Keep movies that are tagged as "funny"
funny_movies = first_genre               .join(tag_df, "movieId", "inner")               .filter(lower(tag_df["tag"]).contains("funny"))               .select("movieId", "tag", "genre")               .dropDuplicates(["movieId"])

# Keep movies that are rated above 3.5
good_movies = first_genre              .join(rating_df, "movieId", "inner")              .groupBy("movieId")              .agg({"rating": "mean"})              .withColumnRenamed("avg(rating)", "average_rating")              .filter(col("average_rating") > 3.5)              .select("movieId", "average_rating")

# Keep movies that are tagged as "funny" and are rated above 3.5 and group them by genre
query_10_results = funny_movies                   .join(good_movies, "movieId", "inner")                   .groupBy("genre")                   .agg({"*": "count"})                   .withColumnRenamed("count(1)", "total_movies")                   .sort("genre")                   .select("genre", "total_movies")

# Show results
query_10_results.show()

# Stop measuring performance
stagemetrics.end()

# Print performance metrics
print(stagemetrics.report().split('\n')[6])


# In[22]:


spark.stop()


# In[ ]:




