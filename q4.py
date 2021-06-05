#  ### Query 4

# In[15]:


# Start measuring performance
#stagemetrics.begin()

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
#stagemetrics.end()

# Print performance metrics
#print(stagemetrics.report().split('\n')[6])
