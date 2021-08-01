#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as py


# In[2]:


def load_n_process(file_path):

    #Chain 1: Loading Data, removing unnecessary columns, and renaming necessary columns.
    df1 = (
        pd.read_csv(file_path, encoding = 'ISO-8859-1', low_memory=False)
        .drop(columns=['NWCG_REPORTING_UNIT_NAME', 'FPA_ID', 'SOURCE_SYSTEM_TYPE', 'SOURCE_SYSTEM', 'NWCG_REPORTING_AGENCY', 'NWCG_REPORTING_UNIT_ID', 'SOURCE_REPORTING_UNIT', 'SOURCE_REPORTING_UNIT_NAME', 'LOCAL_FIRE_REPORT_ID', 'LOCAL_INCIDENT_ID', 'FIRE_CODE', 'ICS_209_PLUS_INCIDENT_JOIN_ID', 'ICS_209_PLUS_COMPLEX_JOIN_ID', 'MTBS_ID', 'MTBS_FIRE_NAME', 'COMPLEX_NAME', 'DISCOVERY_DOY', 'NWCG_CAUSE_AGE_CATEGORY', 'OWNER_DESCR', 'COUNTY', 'NWCG_GENERAL_CAUSE'])
        .rename(columns = {'NWCG_CAUSE_CLASSIFICATION':'CAUSE'})
        .dropna()
    )
    
    df2 = df1
    
    #Chain 2: Dropping unnecessary rows, and resetting the index to aid in analysis.
    df2 = (
        df2.drop(df2[(df2['FIRE_SIZE_CLASS']=='A')].index)
        .drop(df2[(df2['FIRE_SIZE_CLASS']=='B')].index)
        .reset_index()
        
    )

    df=df2
    return df


# In[ ]:




