# ### Query 10

# In[21]:


# Start measuring performance
#stagemetrics.begin()

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
#stagemetrics.end()

# Print performance metrics
#print(stagemetrics.report().split('\n')[6])


