# ### Query 8

# In[19]:


# Start measuring performance
#stagemetrics.begin()

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
#stagemetrics.end()

# Print performance metrics
#print(stagemetrics.report().split('\n')[6])

