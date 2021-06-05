#!/usr/bin/env python
# coding: utf-8

# # Question 1

# ## Initiate Spark

# In[1]:


from pyspark.sql import SparkSession
#from sparkmeasure import StageMetrics
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Create a new Spark Session
spark = SparkSession        .builder        .master("spark://localhost:7077")        .appName("movies")        .getOrCreate()

# Create spark metrics object
#stagemetrics = StageMetrics(spark)


# In[2]:


#spark


# ## Load Datasets To Dataframes

# ### Movie Dataframe

# In[3]:


movie_df = (spark
           .read
           .format("csv")
           .option("header", "true")
           .option("delimiter", ",")
           .option("inferSchema", "true")
           .load("/home/administrator/Downloads/movielens/movie.csv")
           )


# In[4]:


#movie_df.printSchema()


# In[5]:


#movie_df.show(5)


# ### Rating Dataframe

# In[6]:


rating_df = (spark
            .read
            .format("csv")
            .option("header", "true")
            .option("delimiter", ",")
            .option("inferSchema", "true")
            .load("/home/administrator/Downloads/movielens/rating.csv")
            )


# In[7]:


#rating_df.printSchema()


# In[8]:


#rating_df.show(5)


# ### Tag Dataframe

# In[9]:


tag_df = (spark
         .read
         .format("csv")
         .option("header", "true")
         .option("delimiter", ",")
         .option("inferSchema", "true") 
         .load("/home/administrator/Downloads/movielens/tag.csv")
         )


# In[10]:


#tag_df.printSchema()


# In[11]:


#tag_df.show(5)

