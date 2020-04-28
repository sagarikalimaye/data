#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkContext
from pyspark.sql import SparkSession
sc = SparkContext()
spark = SparkSession(sc)

import pandas as pd
import geopandas as gpd


# In[48]:


boroughs = gpd.read_file('boroughs.geojson').to_crs(fiona.crs.from_epsg(2263))
nh = gpd.read_file('neighborhoods.geojson').to_crs(fiona.crs.from_epsg(2263))
print(boroughs.head())
print(nh.head())


# In[42]:


taxi = sc.textFile('yellow.csv.gz')
list(enumerate(taxi.first().split(',')))


# In[50]:


import matplotlib
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')

import seaborn as sns
sns.set(style="whitegrid")

import fiona
import fiona.crs
import shapely
import rtree

index = rtree.Rtree()
for idx,geometry in enumerate(nh.geometry):
    index.insert(idx, geometry.bounds)
    
print(index.bounds)

index2 = rtree.Rtree()
for idx,geometry in enumerate(boroughs.geometry):
    index2.insert(idx, geometry.bounds)

print(index2.bounds)


# In[64]:


import os
os.environ['PROJ_LIB']=r"C:\Users\sagar\Anaconda3\Library\share" 
import csv
import pyproj
import shapely.geometry as geom
proj = pyproj.Proj(init="epsg:2263", preserve_units=True)    

import gzip
# with gzip.open('/home/joe/file.txt.gz', 'rb') as f:
#     header = f.readline()
counts = {}
with gzip.open('yellow.csv.gz', 'rt') as fi:
    reader = csv.reader(fi)
    print(next(reader)) # Skip the header, and print it out for information
    i=0
    for row in reader:
        p = geom.Point(proj(float(row[3]), float(row[2])))
        match = None
        for idx in index.intersection((p.x, p.y, p.x, p.y)):
            # idx is in the list of shapes that might match
            if nh.geometry[idx].contains(p):
                match = idx
                break
        if match:
            if match in counts:
                counts[match][0]+=1
            else:
                counts[match]=[1,0]
            #counts[match] = counts.get(match, 0) + 1
                for idx in index2.intersection((p.x, p.y, p.x, p.y)):
                    if boroughs.geometry[idx].contains(p):
                        #print(idx)
                        counts[match][1]=idx 
print(counts)


# In[65]:


countsPerNeighborhood = list(map(
    lambda x: (nh['neighborhood'][x[0]], x[1]),
    counts.items()))


# In[116]:


from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col
def toCSV(_, records):
    for x in records:
        if x[0][1] == 0:
            product = '"{}"'.format(product)
        yield ','.join((product,year,str(total),str(companies),str(top_percent)))


rdd = sc.parallelize(countsPerNeighborhood) 
df = spark.createDataFrame(rdd)
df1 = Window.partitionBy(df[1][1]).orderBy(df[1][0].desc())

df = df.select('*', rank().over(df1).alias('rank')).filter(col('rank') <= 3)


# In[122]:


from pyspark.sql import functions as f
from pyspark.sql import types as t

def newCols(x):
    return names[x]
finaldf = f.udf(newCols, t.StringType())
names=  ['staten island', 'Queens','Brooklyn','Manhattan','Bronx' ]


df2 = df.withColumn('borough', finaldf(df[1][1]))


# In[127]:


df2 = df2.withColumn('boroughsss', f.concat(df2[0],f.lit(","),df2[1][0]))
df2.show()


# In[129]:


df3 = df2.groupBy('borough').agg(f.collect_list('boroughsss').alias('savitha'))
df3.show()
df4 = df3.withColumn('savitha',f.concat_ws(",","savitha"))
df4.show()


# In[ ]:


df4.write.csv("final1qq")

