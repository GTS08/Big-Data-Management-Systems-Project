# ### Query 2

# In[13]:


# Start measuring performance
#stagemetrics.begin()

# Get the movieIds with tags containing the word "boring"
unique_boring_movieIds = tag_df                         .filter(lower(tag_df["tag"]).contains("boring"))                         .select("movieId")                         .dropDuplicates()

# Get the corresponding movie titles from movieIds in alphabetical order
query_2_results = unique_boring_movieIds                  .join(movie_df, "movieId", "inner")                  .select(movie_df.title)                  .sort(movie_df.title)

# Show results
query_2_results.show()

# Stop measuring performance
#stagemetrics.end()

# Print performance metrics
#print(stagemetrics.report().split('\n')[6])

