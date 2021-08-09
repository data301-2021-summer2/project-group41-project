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

#defining function for Brandon's datasets
def load_and_process_brandon(filepath1,filepath2,filepath3):
    dff = pd.read_csv(filepath1, encoding = 'ISO-8859-1', low_memory=False)
   #Dropping all irrelevant columns and data via method chain
    dff = (
        dff.drop(columns=["FOD_ID","FPA_ID","SOURCE_SYSTEM_TYPE","SOURCE_SYSTEM","NWCG_REPORTING_AGENCY","NWCG_REPORTING_UNIT_ID","NWCG_REPORTING_UNIT_NAME","SOURCE_REPORTING_UNIT","SOURCE_REPORTING_UNIT_NAME","LOCAL_FIRE_REPORT_ID","LOCAL_INCIDENT_ID","FIRE_CODE","FIRE_NAME","ICS_209_PLUS_INCIDENT_JOIN_ID","ICS_209_PLUS_COMPLEX_JOIN_ID","MTBS_ID","MTBS_FIRE_NAME","COMPLEX_NAME","DISCOVERY_DATE","DISCOVERY_DOY","DISCOVERY_TIME","NWCG_GENERAL_CAUSE","NWCG_CAUSE_AGE_CATEGORY","CONT_DATE","CONT_DOY","CONT_TIME","LATITUDE","LONGITUDE","OWNER_DESCR","COUNTY","FIPS_CODE","FIPS_NAME"])
        .drop(dff[dff.NWCG_CAUSE_CLASSIFICATION == 'Human'].index)
        .drop(dff[dff.NWCG_CAUSE_CLASSIFICATION == 'Missing data/not specified/undetermined'].index)
        .dropna()
        .drop(columns=['NWCG_CAUSE_CLASSIFICATION'])
        )
    dff = dff.drop(dff[(dff['FIRE_SIZE_CLASS'] == 'A')].index)
    dff.head()


#Cleaning up data via short method chain
    dfstate = pd.read_csv(filepath2)
    dfstate = (
        dfstate.drop(columns='Abbrev')
        .rename(columns={'Code':'STATE'})
        )
    
    
#Merging and cleaning up data   

    df3 = (
        dff.set_index('STATE').combine_first(dfstate.set_index('STATE'))
        .reset_index()
        .drop(columns='STATE')
        .dropna()
        .rename(columns={'FIRE_YEAR':'Date','State':'Location'})
        )
    df3['Date'] = df3['Date'].astype(int)
    df3


#Reading in third data set, I found it easier not to use a method chain due to the individual column applications
    dftemp=pd.read_csv(filepath3)
    f = lambda dftemp : dftemp['Location'].split("CD")
    dftemp['Location'] = dftemp.apply(f, axis=1)
    g = lambda dftemp : dftemp['Location'].pop(0)
    dftemp['Location'] = dftemp.apply(g, axis=1)
    dftemp.reset_index()
    dftemp = dftemp.drop(columns=['Location ID','Rank','Anomaly (1901-2000 base period)','1901-2000 Mean'])
    dftemp['Location'] = dftemp['Location'].str.rstrip()
    dftemp['Celsius'] = (dftemp['Value']-32)*5/9
    dftemp['Celsius'] = dftemp['Celsius'].round(1)
    dftemp = dftemp.drop(columns=['Value'])
    dftemp = dftemp.drop(dftemp[dftemp['Date'] % 10 != 7].index)
    dftemp['Date'] = dftemp['Date']//100
    dftemp['Date'] = dftemp['Date'].astype(int)
    dftemp = dftemp.drop(dftemp[dftemp.Date > 2018].index)
    dftemp = dftemp.drop(dftemp[dftemp['Date'] < 1992].index)
    dftemp = dftemp.reset_index(drop = True)
    dftemp = dftemp.reset_index(drop = False)


#Cleaning up merged dataframe
    dffinal = (
        df3.groupby(['Date','Location']).size()
        .to_frame()
        .reset_index()
    )
    df3.drop(df3[df3.Location == 'Alaska'].index)
    df3.drop(df3[df3.Location == 'Hawaii'].index)
    dffinal = (    
        dffinal.sort_values(by=['Location','Date'])
        .rename(columns={0: '# of Fires'})
        .reset_index(drop=True)
         .reset_index(drop=False)
        )
#Matching indexes of 2 dataframes for ease of merging, created ID to merge on
    d1=dffinal
    d1 = d1.drop(columns=['index'])
    d1['Date']=d1['Date'].values.astype(str)
    d1['Location']=d1['Location'].values.astype(str)
    d1['id'] = d1['Date'].str.cat(d1['Location'],sep='-')
    d1

#Matching indexes of 2 dataframes for ease of merging, created ID to merge on
    dftemp12 = dftemp
    dftemp12 = dftemp12.drop(columns=['index'])
    dftemp12['Date']=dftemp12['Date'].values.astype(str)
    dftemp12['Location']=dftemp12['Location'].values.astype(str)
    dftemp12['id'] = dftemp12['Date'].str.cat(dftemp12['Location'],sep='-')
    dftemp100 = dftemp12.drop(columns=['Date','Location'])
    dftemp100
#Merging final data frame
    dffinal = pd.merge(d1,dftemp100,how='right',on='id')
    dffinal = dffinal.drop_duplicates(subset=['id'])
    dffinal = dffinal.dropna()
    dffinal = dffinal.reset_index()
#Final data frame
    return dffinal


