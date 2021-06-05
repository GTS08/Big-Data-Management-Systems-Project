# ### Query 6

# In[17]:


# Start measuring performance
#stagemetrics.begin()

# Group rating by movieId and get movie title from inner join
query_6_results = rating_df                  .groupBy("movieId")                  .agg({"rating": "count"})                  .join(movie_df, "movieId", "inner")                  .select(["title", "count(rating)"])                  .sort("count(rating)", ascending=False)                  .withColumnRenamed("count(rating)", "total_ratings")

# Show results
query_6_results.show()

# Stop measuring performance
#stagemetrics.end()

# Print performance metrics
#print(stagemetrics.report().split('\n')[6])
