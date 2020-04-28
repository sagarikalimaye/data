#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkContext
from pyspark.sql import SparkSession
sc = SparkContext()
spark = SparkSession(sc)

import fiona
import fiona.crs
import shapely
import rtree

import pandas as pd
import geopandas as gpd

import sys

import matplotlib
import matplotlib.pyplot as plt
get_ipython().run_line_magic('matplotlib', 'inline')

import seaborn as sns
# In[2]:
def main():

    boroughs = gpd.read_file('boroughs.geojson').to_crs(fiona.crs.from_epsg(2263))
    nh = gpd.read_file('neighborhoods.geojson').to_crs(fiona.crs.from_epsg(2263))
    print(boroughs.head())
    print(nh.head())
    
    
    # In[3]:
    
    
    #taxi = sc.textFile('yellow.csv.gz')
    #list(enumerate(taxi.first().split(',')))
    
    
    # In[4]:
    
    
    
    sns.set(style="whitegrid")
    
    
    index = rtree.Rtree()
    for idx,geometry in enumerate(nh.geometry):
        index.insert(idx, geometry.bounds)
        
    print(index.bounds)
    
    index2 = rtree.Rtree()
    for idx,geometry in enumerate(boroughs.geometry):
        index2.insert(idx, geometry.bounds)
    
    print(index2.bounds)
    
    
    # In[5]:
    
    
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
    # with gzip.open(sys.argv[1], 'rt') as fi:
    #     reader = csv.reader(fi)
    with open(sys.argv[1], 'r') as fi:
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
    
    
    # In[6]:
    
    
    countsPerNeighborhood = list(map(
        lambda x: (nh['neighborhood'][x[0]], x[1]),
        counts.items()))
    
    
    # In[7]:
    
    
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
    
    
    # In[9]:
    
    
    from pyspark.sql import functions as f
    from pyspark.sql import types as t
    
    def newCols(x):
        return names[x]
    finaldf = f.udf(newCols, t.StringType())
    names=  ['staten island', 'Queens','Brooklyn','Manhattan','Bronx' ]
    
    
    df2 = df.withColumn('borough', finaldf(df[1][1]))
    
    
    # In[10]:
    
    
    df2 = df2.withColumn('boroughsss', f.concat(df2[0],f.lit(","),df2[1][0]))
    df2.show()
    
    
    # In[11]:
    
    
    df3 = df2.groupBy('borough').agg(f.collect_list('boroughsss').alias('locations'))
    df3.show()
    df4 = df3.withColumn('locations',f.concat_ws(",","locations"))
    df4.show()
    
    
    # In[ ]:
    
    
    df4.write.csv(sys.argv[2])

if __name__=='__main__':
    sc = SparkContext()
    main(sc)
