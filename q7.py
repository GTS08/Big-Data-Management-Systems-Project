# ### Query 7

# In[18]:


# Start measuring performance
#stagemetrics.begin()

# Group by year and userId and count the ratings for each group
grouped_data = rating_df               .withColumn("year", year(rating_df["timestamp"]))               .groupBy(["year", "userId"])               .agg({"rating": "count"})

# Create a window to limit ratings
window = Window         .partitionBy(grouped_data["year"])         .orderBy(grouped_data["count(rating)"]         .desc())

# Limit the ratings in each group
query_7_results = grouped_data                  .select('*', row_number().over(window).alias('row_number'))                  .filter(col('row_number') <= 10)                  .select(["year", "userId", "count(rating)"])                  .sort(["year", "count(rating)"], ascending = [True, False])                  .withColumnRenamed("count(rating)", "total_ratings")

# Show results
query_7_results.show()

# Stop measuring performance
#stagemetrics.end()

# Print performance metrics
#print(stagemetrics.report().split('\n')[6])
