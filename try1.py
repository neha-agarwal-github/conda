#!/usr/bin/env python
# coding: utf-8

# In[1]:


pip install dask[complete]


# In[2]:


pip install dask distributed --upgrade


# In[3]:


pip install fastparquet


# In[4]:


pip install gcsfs


# In[1]:


import os
import pandas as pd
import dask.dataframe as dd
import fastparquet
import gcsfs
from fastparquet import ParquetFile
from distributed import Client, LocalCluster


# In[2]:


cluster=LocalCluster(n_workers=10)
client=Client(cluster)


# In[3]:


fs=gcsfs.GCSFileSystem(token='anon')
f=fs.open("anaconda-public-data/nyc-taxi/nyc.parquet/part.0.parquet")
pf=ParquetFile(f)
df1=pf.to_pandas()
df2=dd.from_pandas(data=df1,npartitions=3)
df_clust=client.persist(df2)


# In[41]:


df_clust.head(5)


# In[40]:


df3=df_clust.groupby(df_clust.passenger_count).trip_distance.mean().compute


# In[39]:


df4=df_clust.groupby(df_clust.tpep_pickup_datetime.hour)
df4.first


# In[ ]:





# In[37]:


df11=dd.read_parquet(path="gcs://anaconda-public-data/nyc-taxi/nyc.parquet/part.0.parquet", engine='fastparquet')


# In[35]:


df11.head(5)


# In[ ]:




