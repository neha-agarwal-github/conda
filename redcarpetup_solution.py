#!/usr/bin/env python
# coding: utf-8

# pip install dask[complete]

# pip install dask distributed --upgrade

# pip install fastparquet

# pip install gcsfs

# In[15]:


import pandas as pd
import os
import dask.dataframe as dd
import gcsfs
import fastparquet
from distributed import Client, LocalCluster


# In[16]:


#Create Cluster with 10 workers
cluster=LocalCluster(n_workers=10)
client=Client(cluster)


# In[17]:


#Load Dataset1
df1=dd.read_parquet(path="gcs://anaconda-public-data/nyc-taxi/nyc.parquet/part.0.parquet", engine='fastparquet')


# In[18]:


#Persist Dataset1
df1_clust=client.persist(df1)
df1_clust.head()


# In[20]:


#Load Dataset2
df2 = pd.read_csv("https://s3.amazonaws.com/nyc-tlc/misc/taxi%20_zone_lookup.csv")
df2.head()


# #Extract the hour value from the date time format
# def hr_func(ts):
#     return ts.hour
# 
# df1_clust['tpep_pickup_time_hour'] = df1_clust['tpep_pickup_datetime'].apply(hr_func)

# In[26]:


#Group the observations by pickup hour and calculate the total rides taken in each hour
df1_clust_hour = df1_clust.groupby('tpep_pickup_time_hour').count().compute()
df1_clust_hour['tpep_pickup_datetime']


# In[ ]:


#From the above output,
#Peak hours are:
#Midnight-1AM, 1AM-2AM and 2AM-3AM followed by 6PM-7PM
#Peak Hours from 6PM-7PM is logical to understand, howeve peak hour from midnight-3AM doesnt look realistic. We need to look
#into the data to understand this pattern.

#Off-peak hours are from 5AM-6AM


# In[33]:


from datetime import datetime


# In[57]:


#Calculate the number of rides on 2015-01-01 from midnight-3AM
ret=df1_clust[(df1_clust['tpep_pickup_datetime'] >= '2015-01-01 00:00:00') & (df1_clust['tpep_pickup_datetime'] <= '2015-01-01 02:59:59')]
ret['tpep_pickup_datetime'].count().compute()


# In[ ]:


#Total rides from Midnight to 3AM = 1,57,527
#Total rides from Midnight to 3AM on 2015-01-01 = 88087
#Conclusion: More than 50% of the total rides in the timeframe of midnight to 3AM are on 2015-01-01 which can be explained due
#to the New Year Eve. The data is highly skewed at midnight on 2015-01-01 causing it to be the peak hours.

#Dataset2 provides the Location ID and corresponding zones, which can be used to determine the peak and off-peak hours in 
#specific zones. However there is no variable linking this dataset to dataset1. Hence it is not used for analysis.

