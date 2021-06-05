# ### Query 9

# In[20]:


# Start measuring performance
#stagemetrics.begin()

# Group by year, month, day of month, hour and movieId and find the concurrent viewers for each movie
grouped_data = rating_df               .withColumn("year", year(rating_df["timestamp"]))               .withColumn("month", month(rating_df["timestamp"]))               .withColumn("dayofmonth", dayofmonth(rating_df["timestamp"]))               .withColumn("hour", hour(rating_df["timestamp"]))               .groupBy(["movieId","year", "month", "dayofmonth", "hour"])               .agg({"*": "count"})               .withColumnRenamed("count(1)", "concurrent_viewers")               .filter(col("concurrent_viewers") > 1)

# Get the sum of concurrent viewers
query_9_result = grouped_data.select(sum("concurrent_viewers"))

# Show result
query_9_result.show()

# Stop measuring performance
#stagemetrics.end()

# Print performance metrics
#print(stagemetrics.report().split('\n')[6])


