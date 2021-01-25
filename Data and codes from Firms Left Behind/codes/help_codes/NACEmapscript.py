import pandas as pd
import math
import glob

#remove last two digits from number
def rounddown(x):
    x=round(x)
    return int(math.floor(x / 100.0))
    
list=glob.glob('./*.dta')
for i in list:
    fname=i[2:-4]#extract name
    print(i)
    data = pd.io.stata.read_stata(fname+'.dta')#load the table 

    #Data cleaning
    data=data.dropna()
    data=data[pd.to_numeric(data['NACERev2primarycode'], errors='coerce').notnull()]
    
    #a=data.sort_values(by=['NACERev2primarycode'],ascending=True)
    #4digit to 2digit
    data['NACERev11primarycode']=data['NACERev11primarycode'].map(lambda name: rounddown(name))
    data['NACERev2primarycode']=data['NACERev2primarycode'].map(lambda name: rounddown(name))
    
    #sum number of employees per unique nace11*nace2 combination on 2 digit level
    data=data.groupby(['NACERev11primarycode','NACERev2primarycode']).sum()
    d=data.drop(['num'], axis=1)
    #compute totals per nace11 resp. nace 2 category
    map1to2sum=d.groupby(['NACERev11primarycode']).sum()
    map2to1sum=d.groupby(['NACERev2primarycode']).sum()
    
    #compute shares per nace11 resp. nace 22 categories
    d12=d.join(map1to2sum, on='NACERev11primarycode', how='left', lsuffix='_cross', rsuffix='_total') 
    d21=d.join(map2to1sum, on='NACERev2primarycode', how='left', lsuffix='_cross', rsuffix='_total')
    d12['Share']=d12['Employees2007_cross']/d12['Employees2007_total']
    d21['Share']=d21['Employees2007_cross']/d21['Employees2007_total']
    d12.drop(columns=['Employees2007_cross','Employees2007_total'], inplace=True)
    d21.drop(columns=['Employees2007_cross','Employees2007_total'], inplace=True)
    
    #save the mappings
    d12.to_csv(fname+'1to2.csv')
    d21.to_csv(fname+'2to1.csv')