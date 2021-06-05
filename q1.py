# ### Query 1

# In[12]:


# Start measuring performance
#stagemetrics.begin()

# Get the id of the movie "Jumanji"
jumanji_id = movie_df             .filter(movie_df.title.contains("Jumanji"))             .select("movieId")             .collect()[0]["movieId"]

# Get the number of users that watched "Jumanji"
query_1_result = rating_df                 .filter(rating_df["movieId"] == jumanji_id)                 .select(count("movieId"))

# Show result
query_1_result.show()

# Stop measuring performance
#stagemetrics.end()

# Print performance metrics
#print(stagemetrics.report().split('\n')[6])
