# ### Query 3

# In[14]:


# Start measuring performance
#stagemetrics.begin()

# Get the userIds and movieIds with tags containing the word "bollywood"
bollywood_userIds_movieIds = tag_df                             .filter(lower(tag_df["tag"]).contains("bollywood"))                             .select(["userId", "movieId", "tag"])

# Get all userIds and movieIds with rating above 3
above_3_rating = rating_df                 .filter(rating_df.rating > 3)                 .select(["userId", "movieId", "rating"])

# Inner join based on unique combination of userId and movieId
query_3_results = bollywood_userIds_movieIds                  .join(above_3_rating, ["userId", "movieId"], "inner")                  .select("userId")                  .dropDuplicates()                  .sort(above_3_rating.userId)

# Show results
query_3_results.show()

# Stop measuring performance
#stagemetrics.end()

# Print performance metrics
#print(stagemetrics.report().split('\n')[6])
