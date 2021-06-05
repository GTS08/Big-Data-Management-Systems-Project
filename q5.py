# ### Query 5

# In[16]:


# Start measuring performance
#stagemetrics.begin()

# Get the id and title of 2015 movies
movies_2015 = movie_df              .filter(movie_df.title.contains("(2015)"))              .select(["movieId", "title"])

# Get the tags of 2015 movies
joined = movies_2015         .join(tag_df, "movieId", "inner")         .select(["title", "tag"])         .sort("title")

# Group tags by movie title and concatenate them
query_5_results = joined                  .groupby("title")                  .agg(concat_ws(", ", collect_list(joined.tag)).alias("tags"))

# Show results
query_5_results.show()

# Stop measuring performance
#stagemetrics.end()

# Print performance metrics
#print(stagemetrics.report().split('\n')[6])
