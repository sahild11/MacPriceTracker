# -*- coding: utf-8 -*-
"""
Created on Tue Aug 28 13:41:34 2018

@author: Sahil.Dilwali
"""
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 21

@author: Sahil.Dilwali
"""
import datetime
print('Begin processing:',datetime.datetime.now())

import os
os.chdir('C:\\Users\\Sahil.Dilwali\\OneDrive - Shell\\20180716 Mid Year Update '+\
         'Mockup\\20180828 Programming and Staging Datasets')
import pandas as pd

from simple_salesforce import Salesforce
import pickle
pd.set_option('display.max_columns',500)
import sf_login
sf_prod=sf_login.sf_login()
MMP_Files=['20180820 Site Detail P2 2018.xlsx',
           '20180828 Site Detail P3 2017.xlsx']

#Date range for queries
tdate=str(datetime.datetime.today().date())
cyr=int(str(datetime.datetime.now())[0:4])
pyr=cyr-1
cmon=int(str(datetime.datetime.now())[5:7])
pmon=cmon-1
if cmon<3:
    pmon=12
    cyr=cyr-1
Period=1
if pmon>5: Period = 2
if pmon>9: Period = 3
if pmon<10:
    pmon='0'+str(pmon)
else:
    pmon=str(pmon)

cyr_start= str(cyr)+'-01-01'
cyr_end  = str(cyr)+'-'+pmon+'-01'
pyr_start= str(cyr-1)+'-01-01'
pyr_end  = str(cyr-1)+'-'+pmon+'-01'

                                 



#PMTDR List
PMTDR=pd.read_excel('20180827 PMTDR Master List.xlsx',
                    sheet='PMTDR_MASTER LIST 2018')
PMTDR.columns
keep_cols=['OPCO', 'CO/DO','LOCATION NO']
PMTDR=PMTDR[keep_cols]
PMTDR['LOCATION NO']=PMTDR['LOCATION NO'].astype(str)
PMTDR.rename(inplace=True,index=str,columns={'LOCATION NO':'Location_Number'})
#PMTDR1=PMTDR[['LOCATION NO']]





#Location Hierarchy
try:
    del Locations
except Exception as e:
    print(e)
try:
    Locations=pickle.load(open(tdate+'_location_hierarchy.pkl','rb'))
except Exception as e:
    #Get hierarchy data
    print('Pulling Hierarchy Data:',str(datetime.datetime.now()))
    Locations=sf_prod.query_all("SELECT Location_Number__c, Sold_To_Name__c, \
                                   Sold_To_Number__c, Street__c, City__c \
                                   FROM Location_Master__c \
                                   where Sold_To_Name__c != ''")
    print('Pulled Hierarchy Data:',str(datetime.datetime.now()))
    print(Locations['totalSize'])
    records = [dict(Location_Number=rec['Location_Number__c'],
                    Sold_To_Name=   rec['Sold_To_Name__c'],
                    Sold_To_Number= rec['Sold_To_Number__c'],
                    Address=        rec['Street__c'],
                    City=           rec['City__c'])
    for rec in Locations['records']]
    Locations=pd.DataFrame(records)
    Locations.describe
    del records
    print('Created Hierarchy Data Set:',str(datetime.datetime.now()))
    pickle.dump(Locations,open(tdate+'_location_hierarchy.pkl','wb'))
#Merge with PMTDR list
PMTDR=PMTDR.merge(Locations,on='Location_Number',how='left')
PMTDR.head()
del Locations





#### MMP Data import process
def pull_mmp_from_excel():
    for MMP_File in MMP_Files:
        MMPt=pd.read_excel(MMP_File)
        MMPt.columns=[x.upper() for x in MMPt]
        MMPt.columns=['LOCATION#' if x=='LOCATIONCODE' else x for x in MMPt]
        MMPt.columns=[x.replace('_RESPONSE','') for x in MMPt]
        for col in MMPt: print(MMP_File, col)
        if MMPt['WAVID'][0].astype(str)[0:4]=='2017':
            MMPt = MMPt[['LOCATION#','WAVID','TERRITORY MANAGER',
                         'SALES MANAGER','Q7','Q8','Q9','Q10','Q11','Q12',
                         'Q15','Q19','SESCORE']]
            MMPt.rename(index=str, columns={'Q15':'Q14','Q19':'Q18'},inplace=True)
        else:
            if MMPt['WAVID'][0].astype(str)[0:4]=='2018':
                MMPt = MMPt[['LOCATION#','WAVID','TERRITORY MANAGER',
                             'SALES MANAGER','Q7','Q8','Q9','Q10','Q11',
                             'Q12','Q14','Q18','SESCORE']]
            else: print(MMP_File,' wavid not in 2017 or 2018')
        try: MMP=MMP.append(MMPt,ignore_index=True)
        except Exception as e:
            print('errlocMMP1',e)
            MMP=MMPt.copy()
        del MMPt
    return(MMP)
        
try: del MMP
except Exception as e: print('errlocMMP2:',e)

try:
    MMP=pickle.load(open(tdate+'_raw_MMP.pkl','rb'))
except Exception as e:
    MMP=pull_mmp_from_excel()
    pickle.dump(MMP,open(tdate+'_raw_MMP.pkl','wb'))
#pd.crosstab(MMP['WAVID'],1)
#MMP.groupby('WAVID').count()

"""
pd.crosstab(MMP['Q10'].astype(str),MMP['Q10-Attendant'].astype(str),dropna=False)
pd.crosstab(MMP['Q11'].astype(str),MMP['Q11-Attendant'].astype(str),dropna=False)
pd.crosstab(MMP['Q12'].astype(str),MMP['Q12-Attendant'].astype(str),dropna=False)
pd.crosstab(MMP['Q8'].astype(str),MMP['Q9'].astype(str),dropna=False)
for SRR_Q in ['Q7','Q8','Q9','Q14']:
    print(SRR_Q)
    print(pd.crosstab(MMP['QStore'],MMP[SRR_Q].astype(str)))
"""

#calculate TLAG score
MMP['TLAG']=0; MMP['TLAG_denom']=21
for col in ['Q10','Q11','Q12']:
    MMP.loc[MMP[col]=='Yes','TLAG']=MMP['TLAG']+6
    MMP.loc[MMP[col]=='Caution','TLAG']=MMP['TLAG']+3
    MMP.loc[pd.isnull(MMP[col]),'TLAG_denom']=MMP['TLAG_denom']-6
MMP.loc[MMP['Q18']=='Yes','TLAG']=MMP['TLAG']+3
pd.crosstab(MMP['TLAG_denom'],MMP['TLAG'])
#Check list of 0 scores for TLAG
MMP[MMP['TLAG']==0]

#Calculate SRR Score
MMP['SRR']=0; MMP['SRR_denom']=21
for col in ['Q7','Q8','Q9']:
    MMP.loc[MMP[col]=='Yes','SRR']=MMP['SRR']+6
    MMP.loc[MMP[col]=='Caution','SRR']=MMP['SRR']+3
    MMP.loc[pd.isnull(MMP[col]),'SRR_denom']=MMP['SRR_denom']-6
MMP.loc[MMP['Q14']=='Yes','SRR']=MMP['SRR']+3
MMP.loc[pd.isnull(MMP['Q14']),'SRR_denom']=MMP['SRR_denom']-3
pd.crosstab(MMP['SRR']/MMP['SRR_denom'],MMP['SRR'])
#MMP[MMP['SRR_denom']==6]

#Calculate MMP scores and append
for score_comp in ['TLAG','SRR']:
    MMP[score_comp]=MMP[score_comp]/MMP[score_comp+'_denom']
    del MMP[score_comp+'_denom']
MMP_all=MMP.copy()
for Q in range(7,19):
    try: del MMP['Q'+str(Q)]
    except Exception as e: print('errlocMMP3:',e)
for txt_col in ['LOCATION#','WAVID']:
    MMP[txt_col]=MMP[txt_col].astype(str)
MMP=MMP.pivot('LOCATION#','WAVID',['TLAG','SRR','SESCORE'])
MMP.columns = list(map("_".join, MMP.columns))
MMP.reset_index(inplace=True,drop=False)
MMP.rename(inplace=True,columns={'LOCATION#':'Location_Number'})

#Merge with PMTDR List
PMTDR=PMTDR.merge(MMP,on='Location_Number',how='left')
del MMP





#### VOLUME DATA - GAS AND VPOWER
def pull_vol_from_sf():
    print('Pulling Volume Data:',str(datetime.datetime.now()))
    for loc in PMTDR['Location_Number']:
        Raw=sf_prod.query_all('SELECT Location__r.Location_Number__c, Date__c, \
                              sum(Total_Volume_CC_Inflated__c) a1, \
                              sum(V_Power_Volume_CC_Inflated__c) a2 \
                              FROM Credit_Card_Sales_Metric__c \
                              where Location__r.Location_Number__c=\''+loc+'\' \
                              and Date__c >= '+str(cyr-2)+'-01-01 \
                              group by Location__r.Location_Number__c, Date__c')
        records = [dict(Date=           rec['Date__c'],
                        Gas=            rec['a1'],
                        VPower=         rec['a2'],
                        Location_Number=rec['Location_Number__c'])
                   for rec in Raw['records']]
        Raw=pd.DataFrame(records)
        try:
            Locs_and_Volumes=Locs_and_Volumes.append(Raw, ignore_index=True)
            print('appending')
            print(Locs_and_Volumes.shape)
        except Exception as e:
            print(e)
            Locs_and_Volumes=Raw.copy()
            print('replacing')
    print('Pulled Volume Data:',str(datetime.datetime.now()))
    print(Locs_and_Volumes.shape)
    return(Locs_and_Volumes)
    
#Use pickle if available
try: del Volumes
except Exception as e: print(e)
try:
    Volumes=pickle.load(open(tdate+'_raw_vol.pkl','rb'))
    print('Reading volume data from '+tdate+'_raw_vol.pkl')
except Exception as e:
    #Get volume data - YTD 2017 and YTD 2018
    Volumes=pull_vol_from_sf()
    pickle.dump(Volumes,open(tdate+'_raw_vol.pkl','wb'))

#Function to process and pivot separately
def separate_YTD_processing(df,max_mon,max_yr,prev_period_end):
    print('Limiting data and pivoting for '+str(max_yr)+' and '+str(max_yr-1))
    print('Only using months '+str(prev_period_end+1)+' thru '+str(max_mon))
    df1=df.query('Year=="'+str(max_yr)+'" or Year=="'+str(max_yr-1)+'"')
    df1=df1.query('Month<='+str(max_mon)+' and Month>'+str(prev_period_end))
    #Summarize and pivot YTD
    del df1['Month']
    df_summ1=df1.groupby(['Location_Number','Year'],as_index=False).sum()
    df_pivot1=df_summ1.pivot('Location_Number','Year',
                           [x for x in df_summ1.columns
                            if x not in ['Location_Number','Year']])
    df_pivot1.columns = list(map("_".join, df_pivot1.columns))
    df_pivot1.columns = [x+'PTD'+str(Period) for x in df_pivot1.columns
                        if x not in ['Location_Number']]
    df_pivot1.reset_index(inplace=True)
    
    print('Limiting data and pivoting for other periods')
    print('Excluding months '+str(prev_period_end+1)+' thru '+str(max_mon)+
          ' for '+str(max_yr))
    df2=df.query('Year!="'+str(max_yr)+'" or Month<='+str(prev_period_end))
    df2=df2.copy()
    #Summarize and pivot other periods
    df2.loc[:,'Period']=(df2['Month']+3)//4
    df2.loc[:,'Period']=df2['Year']+df2['Period'].astype(str)
    del df2['Year']; del df2['Month']
    df_summ2=df2.groupby(['Location_Number','Period'],as_index=False).sum()
    df_pivot2=df_summ2.pivot('Location_Number','Period',
                             [x for x in df_summ2.columns
                              if x not in ['Location_Number','Period']])
    df_pivot2.columns = list(map("_".join, df_pivot2.columns))
    df_pivot2.reset_index(inplace=True)
    
    df_pivot=df_pivot1.merge(df_pivot2,on='Location_Number',how='outer')
    return(df_pivot)

#Function to process and pivot if period end coincides with data period
def period_processing(df,max_mon):
    #Summarize and pivot YTD
    df2=df.copy()
    #Summarize and pivot other periods
    df2.loc[:,'Period']=(df2['Month']+3)//4
    df2.loc[:,'Period']=df2['Year']+df2['Period'].astype(str)
    del df2['Year']; del df2['Month']
    df_summ2=df2.groupby(['Location_Number','Period'],as_index=False).sum()
    df_pivot2=df_summ2.pivot('Location_Number','Period',
                             [x for x in df_summ2.columns
                              if x not in ['Location_Number','Period']])
    df_pivot2.columns = list(map("_".join, df_pivot2.columns))
    df_pivot2.reset_index(inplace=True)
    return(df_pivot2)





#Determine if separate YTD processing needed, and pivot
Volumes_YTD=Volumes.copy()
Volumes_YTD['Year']=Volumes_YTD['Date'].str.slice(0,4)
Volumes_YTD['Month']=Volumes_YTD['Date'].str.slice(5,7).astype(int)
max_mon=pd.crosstab(Volumes_YTD['Month'],Volumes_YTD['Year'])
max_yr=max(Volumes_YTD['Year'].astype(int))
max_mon=max_mon.loc[max_mon[str(max_yr)]>0]
max_mon.reset_index(inplace=True)
max_mon=max(max_mon['Month'])
print('Last month in '+str(max_yr)+' with volume data is:',str(max_mon))
#if not full period:
if(max_mon%4)!=0:
    print('End of period does not coincide with end of data')
    prev_period_end=max_mon//4*4
    Vol_pivot=separate_YTD_processing(Volumes_YTD,max_mon,max_yr,
                                      prev_period_end)
    PTD='PTD'
else:
    print('End of period coincides with end of data')
    Vol_pivot=period_processing(Volumes)
    if prev_period_end==0: prev_period_end=12
    PTD=''
#Column names
GasPdCY='Gas_'+str(cyr)+PTD+str(Period)
GasPdPY='Gas_'+str(cyr-1)+PTD+str(Period)
VPPdCY='VPower_'+str(cyr)+PTD+str(Period)
VPPdPY='VPower_'+str(cyr-1)+PTD+str(Period)
LoyaltyPdCY='Loyalty_'+str(cyr)+PTD+str(Period)
LoyaltyPdPY='Loyalty_'+str(cyr-1)+PTD+str(Period)
GGrowth_PdCY='Gas_Growth_'+str(cyr)+PTD+str(Period)
VGrowth_PdCY='VPower_Growth_'+str(cyr)+PTD+str(Period)
LP_PdCY='Loyalty_Pen_'+str(cyr)+PTD+str(Period)
LPS_PdCY='Loyalty_Pen_Score_'+str(cyr)+PTD+str(Period)
GGS_PdCY='Gas_Score_'+str(cyr)+PTD+str(Period)
VGS_PdCY='VPower_Score_'+str(cyr)+PTD+str(Period)

del Volumes_YTD; del Volumes

#Merge with PMTDR List
PMTDR=PMTDR.merge(Vol_pivot,on='Location_Number',how='left')
PMTDR.head()
del Vol_pivot





#Loyalty data
def pull_loyalty_from_sf():
    print('Pulling Loyalty Data:',str(datetime.datetime.now()))
    Raw=sf_prod.query_all('Select reg_redeemer_fuel__c, Date__c, \
                          Location_Master__r.Location_Number__c \
                          FROM Loyalty_Penetration__c \
                          where Date__c >= '+str(pyr)+'-01-01')
    records = [dict(Date=                                 rec['Date__c'],
                    Loyalty=                              rec['Reg_Redeemer_Fuel__c'],
                    Location_Number=rec['Location_Master__r']['Location_Number__c'])
            for rec in Raw['records']]
    Raw=pd.DataFrame(records)
    Raw['Year']=Raw['Date'].str.slice(0,4)
    Raw['Month']=Raw['Date'].str.slice(5,7).astype(int)
    print('Pulled Loyalty Data:',str(datetime.datetime.now()))
    print(Raw.shape)
    return(Raw)
    
#Use pickle if available
try:
    del Loyalty
except Exception as e:
    print(e)
try:
    Loyalty=pickle.load(open(tdate+'_loyalty_vol.pkl','rb'))
    print('Reading loyalty data from '+tdate+'_loyalty_vol.pkl')
except Exception as e:
    #Get volume data - YTD 2017 and YTD 2018
    Loyalty=pull_loyalty_from_sf()
    pickle.dump(Loyalty,open(tdate+'_loyalty_vol.pkl','wb'))

#Determine if separate YTD processing needed, and pivot
Loyalty_YTD=Loyalty.copy()
Loyalty_YTD['Year']=Loyalty_YTD['Date'].str.slice(0,4)
Loyalty_YTD['Month']=Loyalty_YTD['Date'].str.slice(5,7).astype(int)
max_mon=pd.crosstab(Loyalty_YTD['Month'],Loyalty_YTD['Year'])
max_yr=max(Loyalty_YTD['Year'].astype(int))
max_mon=max_mon.loc[max_mon[str(max_yr)]>0]
max_mon.reset_index(inplace=True)
max_mon=max(max_mon['Month'])
print('Last month in '+str(max_yr)+' with volume data is:',str(max_mon))
#if not full period:
if(max_mon%4)!=0:
    print('End of period does not coincide with end of data')
    prev_period_end=max_mon//4*4
    Loy_pivot=separate_YTD_processing(Loyalty_YTD,max_mon,max_yr,
                                      prev_period_end)
else:
    print('End of period coincides with end of data')
    Loy_pivot=period_processing(Loyalty)
    if prev_period_end==0: prev_period_end=12
del Loyalty_YTD; del Loyalty

#Merge with PMTDR List
PMTDR=PMTDR.merge(Loy_pivot,on='Location_Number',how='left')
PMTDR.head()
del Loy_pivot





#PTD KPIs
try:
    if(prev_period_end>0): print('Calculating KPIs based on PTD assumption.')
    print('Data for PTD columns reflects months '+str(prev_period_end+1)+
          ' to '+str(max_mon))
    #Calculate growth and penetration for PTD columns
    gas_growth_col='Gas_Growth_'+str(max_yr)+'PTD'+str(Period)
    vp_growth_col='VPower_Growth_'+str(max_yr)+'PTD'+str(Period)
    lp_col='Loyalty_Pen_'+str(max_yr)+'PTD'+str(Period)
    PMTDR.loc[:,gas_growth_col]=PMTDR['Gas_'+str(max_yr)+'PTD'+str(Period)] / \
                                PMTDR['Gas_'+str(max_yr-1)+'PTD'+str(Period)]-1
    PMTDR.loc[:,vp_growth_col]=PMTDR['VPower_'+str(max_yr)+'PTD'+str(Period)] / \
                               PMTDR['VPower_'+str(max_yr-1)+'PTD'+str(Period)]-1
    PMTDR.loc[:,lp_col]=PMTDR['Loyalty_'+str(max_yr)+'PTD'+str(Period)] / \
                        PMTDR['Gas_'+str(max_yr)+'PTD'+str(Period)]
except Exception as e:
    print('Beginning scoring based on non-PTD assumption.')

#Remaining KPIs
def calc_kpi(df,year,period):
    df1=df.copy()
    loyalty_col='Loyalty_'+str(year)+str(period)
    lp_col='Loyalty_Pen_'+str(year)+str(period)
    gas_col='Gas_'+str(year)+str(period)
    pgas_col='Gas_'+str(year-1)+str(period)
    gas_growth_col='Gas_Growth_'+str(year)+str(period)
    vp_col='VPower_'+str(year)+str(period)
    pvp_col='VPower_'+str(year-1)+str(period)
    vp_growth_col='VPower_Growth_'+str(year)+str(period)
    
    df1.loc[:,lp_col]=df1[loyalty_col]/df1[gas_col]
    df1.loc[:,gas_growth_col]=df1[gas_col]/df1[pgas_col] - 1
    df1.loc[:,vp_growth_col]=df1[vp_col]/df1[pvp_col] - 1
    
    return(df1)

#Calculate KPIs for each period
PMTDR1=PMTDR.copy()
for yr in range(2):
    for period in range(3):
        try:
            PMTDR1=calc_kpi(PMTDR1,max_yr-1+yr,period+1)
        except Exception as e:
            print('ScoreErrLoc1:',e)

PMTDR1.head()

#Assign points and calculate totals for ranking
"""
Volume (Total and VPower) Data: YTD  = Jan to July
Loyalty Data (Regular Redeemer): YTD = Jan to July
MMP Data - Q7-12, 14,18 from P2 2018

1. Total fuel volume: if 2018YTD >= 2017YTD + 2% then 10 else 0
2. VPower Volume: if 2018YTD >= 2017YTD + 3.5% then 20 else 0
3. Fuel Rewards Loyalty Gallons Penetration: if >=10% then 30, else if 5-9.99% 
   then 25, else 0
4. TLAG (Q10+Q11+Q12+Q18): total/21*20
5. Shop & RR (Q7+Q8+Q9+Q14): total/21*20
"""
#PTD scores
try:
    if(prev_period_end>0): print('Scoring based on PTD assumption.')
    print('Data for PTD columns reflects months '+str(prev_period_end+1)+
          ' to '+str(max_mon))
    #Calculate growth and penetration for PTD columns
    gas_growth_col='Gas_Growth_'+str(max_yr)+'PTD'+str(Period)
    gas_score_col='Gas_Score_'+str(max_yr)+'PTD'+str(Period)
    
    vp_growth_col='VPower_Growth_'+str(max_yr)+'PTD'+str(Period)
    vp_score_col='VPower_Score_'+str(max_yr)+'PTD'+str(Period)
    
    lp_col='Loyalty_Pen_'+str(max_yr)+'PTD'+str(Period)
    lp_score_col='Loyalty_Pen_Score_'+str(max_yr)+'PTD'+str(Period)
    
    tlag_col='TLAG_'+str(max_yr)+str(Period)
    tlag_score_col='TLAG_Score_'+str(max_yr)+str(Period)
    srr_col='SRR_'+str(max_yr)+str(Period)
    srr_score_col='SRR_Score_'+str(max_yr)+str(Period)
    
    pmtdr_col='PMTDR_Score_'+str(max_yr)+str(Period)
    
    df1=PMTDR1.copy()
    df1.loc[:,lp_score_col]=0
    df1.loc[df1[lp_col]>=0.05,lp_score_col]=25
    df1.loc[df1[lp_col]>=0.1,lp_score_col]=30
 
    df1.loc[:,gas_score_col]=0
    df1.loc[df1[gas_growth_col]>=0.02,gas_score_col]=10

    df1.loc[:,vp_score_col]=0
    df1.loc[df1[vp_growth_col]>=0.035,vp_score_col]=20
    
    df1.loc[:,tlag_score_col]=df1[tlag_col]*20
    df1.loc[:,srr_score_col]=df1[srr_col]*20
    
    df1.loc[:,pmtdr_col]=df1[lp_score_col]+df1[gas_score_col]+ \
                         df1[vp_score_col]+df1[tlag_score_col]+ \
                         df1[srr_score_col]
    
    PMTDR1=df1.copy()
except Exception as e:
    print('Beginning scoring based on non-PTD assumption.')
    
#Remaining scores
def calc_scores(df,year,period):
    df1=df.copy()
    #Input columns
    lp_col='Loyalty_Pen_'+str(year)+str(period)
    gas_growth_col='Gas_Growth_'+str(year)+str(period)
    vp_growth_col='VPower_Growth_'+str(year)+str(period)
    tlag_col='TLAG_'+str(year)+str(period)
    srr_col='SRR_'+str(year)+str(period)
    #Score columns
    lp_score_col='Loyalty_Pen_Score_'+str(year)+str(period)
    gas_score_col='Gas_Score_'+str(year)+str(period)
    vp_score_col='VPower_Score_'+str(year)+str(period)
    tlag_score_col='TLAG_Score_'+str(year)+str(period)
    srr_score_col='SRR_Score_'+str(year)+str(period)
    pmtdr_col='PMTDR_Score_'+str(year)+str(period)
    
    df1.loc[:,lp_score_col]=0
    df1.loc[df1[lp_col]>=0.05,lp_score_col]=25
    df1.loc[df1[lp_col]>=0.1,lp_score_col]=30
 
    df1.loc[:,gas_score_col]=0
    df1.loc[df1[gas_growth_col]>=0.02,gas_score_col]=10

    df1.loc[:,vp_score_col]=0
    df1.loc[df1[vp_growth_col]>=0.035,vp_score_col]=20
    
    df1.loc[:,tlag_score_col]=df1[tlag_col]*20
    df1.loc[:,srr_score_col]=df1[srr_col]*20
    df1.loc[:,pmtdr_col]=df1[lp_score_col]+df1[gas_score_col]+ \
                         df1[vp_score_col]+df1[tlag_score_col]+ \
                         df1[srr_score_col]
    return(df1)
    

for yr in range(2):
    for period in range(3):
        try:
            PMTDR1=calc_scores(PMTDR1,max_yr-1+yr,period+1)
        except Exception as e:
            print('ScoreErrLoc2:',e)

#Add current rankings
rank_col='Rank_'+str(max_yr)+str(Period)
pmtdr_score_col='PMTDR_Score_'+str(max_yr)+str(Period)
PMTDR1.sort_values(inplace=True,by=['OPCO',pmtdr_score_col],
                   ascending=[True,False])
PMTDR1.loc[:,rank_col]=1
PMTDR1[rank_col]=PMTDR1[['OPCO',rank_col]].groupby(['OPCO']).cumsum()
PMTDR1.sort_values(inplace=True,by=['Sold_To_Name',pmtdr_score_col],
                   ascending=[True,False])
PMTDR1.head()

#output files
#Entire dataset
PMTDR1.to_excel(str(datetime.datetime.now().date())+' Raw PMTDR Scores.xlsx',
              index=False)

#Averages for each region
PMTDR_Score_Cols=['PMTDR_Score_'+str((cyr-1+x)*10+y+1) for x in range(2) for 
                  y in range(3)]
Average_Cols=['Sold_To_Name','OPCO']+ \
              list([x for x in PMTDR_Score_Cols if x in PMTDR1.columns])

Averages=PMTDR1[Average_Cols].copy()
Averages['OPCO']=Averages['OPCO'].str.title().str.strip()
Averages1=Averages.groupby(['Sold_To_Name','OPCO'],as_index=False).mean()
Averages2=Averages.groupby('OPCO',as_index=False).mean()
Averages2.columns=[col[-5:]for col in Averages2]
Averages=Averages1.merge(Averages2,how='left',on='OPCO')

#MMP Averages
MMP_all.head()
MMP_all.rename(inplace=True,index=str,columns={'LOCATION#':'Location_Number'})
MMP_all['WAVID']=MMP_all['WAVID'].astype(str)
MMP=MMP_all.pivot('Location_Number','WAVID',['TLAG','SRR','SESCORE'])
MMP.columns = list(map("_".join, MMP.columns))
MMP.reset_index(inplace=True,drop=False)
MMP_all.sort_values(['Location_Number','WAVID'],inplace=True,ascending=[True,False])
MMP_tm_sm=MMP_all.drop_duplicates('Location_Number',keep='first')
MMP_tm_sm=MMP_tm_sm[['Location_Number','TERRITORY MANAGER']]
OPCO_Assignment=pd.read_excel('20180905 Non PMTDR Sites Region Assignment.xlsx')
OPCO_Assignment=OPCO_Assignment[['TERRITORY MANAGER','OPCO/Region']]
MMP_tm_sm=MMP_tm_sm.merge(OPCO_Assignment,on='TERRITORY MANAGER',how='left')
del MMP_tm_sm['TERRITORY MANAGER']
MMP=MMP_tm_sm.merge(MMP,on='Location_Number',how='right')
MMP=MMP[MMP['OPCO/Region']!='SKIP FOR NOW']
MMP.rename(inplace=True,index=str,columns={'OPCO/Region':'OPCO'})
PMTDR_sites=PMTDR1[['Location_Number']].copy()
PMTDR_sites['PMTDR']=1
MMP['Location_Number']=MMP['Location_Number'].astype(str)
MMP=MMP.merge(PMTDR_sites,on='Location_Number',how='left')
MMP['PMTDR']=MMP['PMTDR'].fillna(0)
MMP_Averages=MMP.groupby(['PMTDR','OPCO'],as_index=False).mean()
MMP_Averages=pd.melt(MMP_Averages,id_vars=['PMTDR','OPCO'],
                     value_vars=[x for x in MMP_Averages
                                 if x not in ['PMTDR','OPCO']])
MMP_Averages['Metric']=MMP_Averages['variable'].apply(
        lambda x: str(x).split('_')[0])
MMP_Averages['Period']=MMP_Averages['variable'].apply(
        lambda x: str(x).split('_')[1])
del MMP_Averages['variable']
MMP_Averages=MMP_Averages.pivot_table(
        index=['PMTDR','OPCO','Period'],columns='Metric',values='value')
MMP_Averages.reset_index(inplace=True)
MMP_Averages['PMTDR_m']='PMTDR'
MMP_Averages.loc[MMP_Averages['PMTDR']==0,'PMTDR_m']='NonPMTDR'
del MMP_Averages['PMTDR']
MMP_Averages=MMP_Averages.pivot_table(
        index=['OPCO','Period'],columns='PMTDR_m',values=[
                x for x in MMP_Averages if x not in ['PMTDR_m','OPCO',
                                                     'Period']])
MMP_Averages.columns = list(map("_".join, MMP_Averages.columns))
MMP_Averages.reset_index(inplace=True)


#Each OPCO and Sold To Name
OPCO_Sold_To=PMTDR1[['OPCO','Sold_To_Name']].drop_duplicates()
OPCO_Sold_To['OPCO']=OPCO_Sold_To['OPCO'].str.title()
PMTDR1.columns
OPCO_cols=['Location_Number','Address','City',GasPdPY,GasPdCY,GGrowth_PdCY,
           VPPdPY,VPPdCY,VGrowth_PdCY,LoyaltyPdCY,
           'SRR_Score_'+str(cyr)+str(Period),
           'TLAG_Score_'+str(cyr)+str(Period),LPS_PdCY,
           'SESCORE_'+str(cyr)+str(Period),'PMTDR_Score_'+str(cyr)+str(Period),
           'Rank_'+str(cyr)+str(Period)]

TLAG_Score_Cols=['TLAG_'+str((cyr-1+x)*10+y+1) for x in range(2) for 
                  y in range(3)]
SEScore_Cols=['SESCORE_'+str((cyr-1+x)*10+y+1) for x in range(2) for 
                  y in range(3)]
SRR_Score_Cols=['SRR_'+str((cyr-1+x)*10+y+1) for x in range(2) for 
                  y in range(3)]
Average_Cols=['Sold_To_Name','OPCO']+ \
              list([x for x in TLAG_Score_Cols if x in PMTDR1.columns])+ \
              list([x for x in SEScore_Cols if x in PMTDR1.columns])+ \
              list([x for x in SRR_Score_Cols if x in PMTDR1.columns])


#Create workbook for each OPCO
os.chdir('C:\\Users\\Sahil.Dilwali\\OneDrive - Shell\\20180716 Mid Year Update '+\
         'Mockup\\20180828 Programming and Staging Datasets\By Sold To')
print("Writing Files:",str(datetime.datetime.now()))
for ix, row in OPCO_Sold_To.iterrows():
    OPCO=row['OPCO']; Sold_To_Name=row['Sold_To_Name']
    dataset=PMTDR1.query('OPCO.str.title()=="'+OPCO+'" and Sold_To_Name=="'+
                         Sold_To_Name+'"')
    dataset=dataset[OPCO_cols]
    writer=pd.ExcelWriter(OPCO+' '+Sold_To_Name+'.xlsx')
    dataset.to_excel(writer,index=False,sheet_name='Raw')
    avgs=Averages.query('OPCO=="'+OPCO+'" and Sold_To_Name=="'+Sold_To_Name+'"')
    avgs=pd.melt(avgs,id_vars=['Sold_To_Name','OPCO'],
                 value_vars=[x for x in avgs if x not in ['Sold_To_Name','OPCO']])
    avgs.loc[:,'Metric']=avgs['Sold_To_Name']
    avgs.loc[avgs['variable'].str.len()==5,'Metric']=avgs['OPCO']+' Region Average'
    del avgs['OPCO']; del avgs['Sold_To_Name']
    avgs['variable']=avgs['variable'].str.slice(-5)
    avgs=avgs.pivot(index='variable',columns='Metric',values='value')
    avgs.reset_index(inplace=True)
    avgs.to_excel(writer,index=False,sheet_name='PMTDR_Averages')
    
    dataset=PMTDR1.query('OPCO.str.title()=="'+OPCO+'" and Sold_To_Name=="'+
                         Sold_To_Name+'"')
    dataset=dataset[Average_Cols]
    dataset=dataset.groupby(['Sold_To_Name','OPCO'],as_index=False).mean()
    dataset=pd.melt(dataset,id_vars=['Sold_To_Name','OPCO'],
                    value_vars=[x for x in dataset if x not in [
                            'Sold_To_Name','OPCO']])
    dataset['Metric']=dataset['variable'].apply(lambda x: str(x).split('_')[0])
    dataset['Period']=dataset['variable'].apply(lambda x: str(x).split('_')[1])
    del dataset['variable']
    dataset=dataset.pivot_table(index=['Sold_To_Name','OPCO','Period'],
                                columns='Metric',values='value')
    dataset.reset_index(inplace=True)
    dataset=dataset.merge(MMP_Averages,on=['OPCO','Period'],how='left')
    dataset.to_excel(writer,index=False,sheet_name='MMP_Averages')
    
    writer.save()

os.chdir('C:\\Users\\Sahil.Dilwali\\OneDrive - Shell\\20180716 Mid Year Update '+\
         'Mockup\\20180828 Programming and Staging Datasets')

print("Done:",str(datetime.datetime.now()))
"""
#'Attachment' worksheet
Attachment = PMTDR.copy()
#PMTDR.columns
att_cols=['Location_Number', 'Address', 'City',
          'Vol_Growth_'+str(cyr)+str(period),
          'Vol_Score_'+str(cyr)+str(period),
          'VP_Growth_'+str(cyr)+str(period),
          'VP_Score_'+str(cyr)+str(period),
          'Loyalty_Pen_'+str(cyr)+str(period),
          'Loyalty_Score_'+str(cyr)+str(period),
          'TLAG_'+str(cyr)+str(period),
          'SRR_'+str(cyr)+str(period),
          'SESCORE_'+str(cyr)+str(period),
          'PMTDR_Score_'+str(cyr)+str(period),
          'Sold_To_Number','Sold_To_Name']
Attachment=Attachment[att_cols]
"""