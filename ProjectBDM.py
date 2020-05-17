#!/usr/bin/env python
# coding: utf-8

# In[1]:


from pyspark import SparkContext
sc = SparkContext()
from pyspark.sql.types import BooleanType
from pyspark.sql.session import SparkSession
import sys
#spark = SparkSession(sc)


# In[2]:


import pyspark.sql.functions as funct



def main(sc):
    spark = SparkSession(sc)

    #b= b.select('L_LOW_HN', 'L_HIGH_HN','FULL_STREE','ST_LABEL','BOROCODE','PHYSICALID')
    b = spark.read.load('centerline (1).csv', format='csv', header = True, inferSchema = True)
    b.head(10)
    b = b.withColumn('New L_LOW_HN',funct.concat_ws('.',funct.split(b['L_LOW_HN'],'-')))
    b = b.withColumn('New R_LOW_HN',funct.concat_ws('.',funct.split(b['R_LOW_HN'],'-')))
    b = b.withColumn('New L_HIGH_HN',funct.concat_ws('.',funct.split(b['L_HIGH_HN'],'-')))
    b = b.withColumn('New R_HIGH_HN',funct.concat_ws('.',funct.split(b['R_HIGH_HN'],'-')))
    #b.select('BOROCODE').distinct().show()
    b= b.select('NEW L_LOW_HN', 'NEW L_HIGH_HN','NEW R_LOW_HN','NEW R_HIGH_HN','FULL_STREE','ST_LABEL','BOROCODE','PHYSICALID')
    b.head(100)
    
    
    # In[7]:
    
    
    a = spark.read.load('small_data1.csv', format='csv', header = True, inferSchema = True)
    #a.head()
    a= a.withColumn('Year',funct.split(a['Issue Date'],'/').getItem(2))
    
    a = a.filter((a['Year']>='2015')| (a['Year']<='2019'))
    a = a.filter(a['House Number'].rlike('^[0-9]*\-*[0-9]*$'))
    a = a.withColumn('New House Number',funct.concat_ws('.',funct.split(a['House Number'],'-')))
    a = a.select('Violation County','New House Number','Street Name','Year')
    a.head(10)
    
    
    
    
    
    def process(c,d,e,f,g,h,i,j,k,l,m,n):
        a = [c,d,e,f]
        b = [g,h,i,j,k,l,m,n]
    #     if a[0] == 'K':
    #         return True
        boro= {3:['K','KINGS','KING','BK'],2:['BX','BRONX'],1:['NY','MAN','MH','NEW Y','NEWY','MN'],5:['R','RICHMOND'],4:['Q','QU','QUEEN','QN','QNS']}
        if c in boro[m]:
    #         return True
            if ((b[4]==a[2]) | (b[5] == a[2])):
                return True
    #             if float(a[1])%2 == 0:
    #                 if ((float(b[3]) <= float(a[1]) ) & (float(b[2] )>= float(row[1]))):
    #                     return True
    #             else:
    #                 if ((float(b[1]) <= float(a[1]) ) & (float(b[0] )>= float(row[1]))):
    #                     return True
        return False
                
    
    acol = a.columns
    bcol = b.columns
    p = funct.udf(process, BooleanType())
    
    v = a.crossJoin(b).where(p(a['Violation County'],a['New House Number'],a['Street Name'],a['Year'],b['NEW L_LOW_HN'], b['NEW L_HIGH_HN'],b['NEW R_LOW_HN'],b['NEW R_HIGH_HN'], b['FULL_STREE'],b['ST_LABEL'],b['BOROCODE'],b['PHYSICALID']) )
   
    
    
    v= v.groupBy("Year","PHYSICALID").count()
    
    
    
    v.show(10)


if __name__=='__main__':
    sc = SparkContext()
    main(sc)
# In[ ]:


        

