from numpy import int64
import pandas as pd
import xlsxwriter
import time

from datetime import datetime

today_now = datetime.now()

DATAPATH="./Status Cleansing/Raw/"

DATAFILE_MARC = 'MARC_GAR.xlsx'
DATAFILE_MARA = 'MARA_GAR.xlsx'
DATAFILE_MVKE = 'MVKE_GAR.xlsx'
DATAFILE_MB52 = 'MB52_GAR.xlsx'

DATAFILE_GAR_PLANTS = 'GAR_Plants.xlsx'

#Load file
print("Loading Raw files into data frame...")
t1 = time.time()

df_MARC = pd.read_excel(DATAPATH + DATAFILE_MARC,engine="openpyxl")
df_MARA = pd.read_excel(DATAPATH + DATAFILE_MARA,engine="openpyxl")
print(df_MARA.columns)
'''
df_MVKE = pd.read_excel(DATAPATH + DATAFILE_MVKE,engine="openpyxl")
'''
df_MB52 = pd.read_excel(DATAPATH + DATAFILE_MB52,engine="openpyxl")
df_GAR_Plants = pd.read_excel('./' + DATAFILE_GAR_PLANTS,engine="openpyxl")

print(df_MARC.head(5),df_MARA.head(5))
df_MARC.info()
df_MARA.info()

output1 = (time.time()-t1)
print('Time taken in seconds loading files: ' + str(output1))



#Formatting files: Convert all the integers to string
t2 = time.time()
'''
df_MARA["Material"]=df_MARA["Material"].apply(str)
df_MARA["X-plant matl status"]=df_MARA["X-plant matl status"].apply(str)
df_MARA["X-distr.chain status"]=df_MARA["X-distr.chain status"].apply(str)


df_MARC["Plant"]=df_MARC["Plant"].apply(str)
df_MARC["Material"]=df_MARC["Material"].apply(str)
df_MARC["Plant-sp.matl status"]=df_MARC["Plant-sp.matl status"].apply(str)

'''

df_MARC= df_MARC[df_MARC['Plant-sp.matl status'].notna()]

df_MARA=df_MARA.astype({"Material":'str',"X-plant matl status":'str',"X-distr.chain status":'str'})
df_MARC["Plant-sp.matl status"]=df_MARC["Plant-sp.matl status"].apply(int64)
df_MARC=df_MARC.astype({"Plant":'str',"Material":'str',"Plant-sp.matl status":'str'})
print(df_MARC.head(5),df_MARA.head(5))
df_MARC.info()
df_MARA.info()


df_MB52["Material"] = df_MB52["Material"].apply(str)
df_MB52["Plant"] = df_MB52["Plant"].apply(str)
df_MB52["Unrestricted"] = df_MB52["Unrestricted"].apply(int)
df_MB52["In Quality Insp."] = df_MB52["In Quality Insp."].apply(int)
df_MB52["Blocked"] = df_MB52["Blocked"].apply(int)
df_GAR_Plants["Plant"]=df_GAR_Plants["Plant"].apply(str)
df_GAR_Plants["Sales Org"]=df_GAR_Plants["Sales Org"].apply(str)


#create an unique key Material/Plant/Number in MARC
df_MARC['Material/Plant'] = df_MARC['Material'] +"/" + df_MARC['Plant']
df_MB52['Material/Plant'] = df_MB52['Material'] +"/" + df_MB52['Plant']

output2 = (time.time()-t2)
print('Time taken in seconds formatting files: ' + str(output2))

#print(df_MB52.head(),df_GAR_Plants.head())

#Filter out plants not supported by GAR team (MFG)
def get_valid_plants(df_raw,df_plant):
    plants_valid = df_plant[df_plant['Status Cleansing Project']=="Yes"]
    is_plants_valid = df_raw['Plant'].isin(plants_valid['Plant'])
    df_plants_valid = df_raw[is_plants_valid]
    return df_plants_valid

def get_valid_sales_org(df_raw,df_plant):
    plants_valid = df_plant[df_plant['Status Cleansing Project']=="Yes"]
    is_sales_org_valid = df_raw['Sales Org'].isin(plants_valid['Sales Org'])
    df_sales_org_valid = df_raw[is_sales_org_valid]
    return df_sales_org_valid



#output dataframe
t3 = time.time()

# MB52 get existing inventory for GAR SKU, groupby Material/Plant
df_MB52_valid = get_valid_plants(df_MB52,df_GAR_Plants)
df_MB52_valid['Sum of Inventory'] = df_MB52_valid['Unrestricted'] + df_MB52_valid['In Quality Insp.'] + df_MB52_valid['Blocked']
df_MB52_groupby=df_MB52_valid.groupby(['Material/Plant']).sum().apply(lambda x:x).reset_index()

#MARC get X-Plant status & Valid from MARA
df_MARC_valid = get_valid_plants(df_MARC,df_GAR_Plants)
df_MARC_valid = pd.merge(df_MARC_valid,df_MARA[['Material','Material Type','X-plant matl status','Valid from']],on='Material',how='left')
df_MARC_valid = pd.merge(df_MARC_valid,df_MB52_groupby[['Material/Plant','Sum of Inventory']],on='Material/Plant',how='left')
df_MARC_valid.rename(columns={"Valid from_x":"Valid from local plant","Valid from_y":"Valid from X-plant"},inplace=True)

df_MARC_valid.loc[(df_MARC_valid['Material Type'].isna()),'Comment']='Ok,raw materials out of scope'
df_MARC_valid.loc[
    (df_MARC_valid['Plant-sp.matl status']==df_MARC_valid['X-plant matl status']) &
    (df_MARC_valid['Comment'].isna())
    ,'Comment']='Ok,local plant status match with X-plant status'

df_MARC_valid.loc[
    (df_MARC_valid['X-plant matl status']=='80') &
    (df_MARC_valid['Sum of Inventory'].notna()) &
    (df_MARC_valid['Comment'].isna())
    ,'Comment']='MDO action, check with planner why X-plant 80 but have inventory'

df_MARC_valid.loc[
    (df_MARC_valid['Plant-sp.matl status']=='80') &
    (df_MARC_valid['Sum of Inventory'].notna()) &
    (df_MARC_valid['Comment'].isna())
    ,'Comment']='MDO action, check with planner why local plant 80 but have inventory'

df_MARC_valid.loc[
    (df_MARC_valid['Plant-sp.matl status']=='80') &
    (df_MARC_valid['Sum of Inventory'].isna()) &
    (df_MARC_valid['Comment'].isna())
    ,'Comment']='Ok, SKU cancelled locally and no inventory'

df_MARC_valid.loc[
    (df_MARC_valid['X-plant matl status']=='80') &
    (df_MARC_valid['Plant-sp.matl status'] !='80') &
    (df_MARC_valid['Comment'].isna())
    ,'Comment']='MDO action, check with planner why Enterprise inactive but local still active'

df_MARC_valid.loc[
    (df_MARC_valid['Plant-sp.matl status'] =='70') &
    (df_MARC_valid['Sum of Inventory'].isna()) &
    (df_MARC_valid['Comment'].isna())
    ,'Comment']='MDO action, check with planner if could cancel the SKU in plant as no existing inventory'

df_MARC_valid.loc[
    (df_MARC_valid['Plant-sp.matl status'] =='70') &
    (df_MARC_valid['Sum of Inventory'].notna()) &
    (df_MARC_valid['Comment'].isna())
    ,'Comment']='Ok, Status 70 awaiting existing inventory clearance'  

df_MARC_valid.loc[
    (df_MARC_valid['Plant-sp.matl status'] =='41') &
    (df_MARC_valid['Comment'].isna())
    ,'Comment']='Ok, Clinical trial'  

df_MARC_valid.loc[
    (df_MARC_valid['Comment'].isna())
    ,'Comment']='MDO action, check with planner as misalignment in X-plant and local plant status'  

output3 = (time.time()-t3)
print('Time taken in seconds output files: ' + str(output3))


#Extract into Excel
t6 = time.time()
excel_writer = pd.ExcelWriter('./Status Cleansing/'+'Status_Cleansing.xlsx',engine='xlsxwriter')
df_MARC_valid.to_excel(excel_writer,index=False,sheet_name="MARC")
df_MB52_valid.to_excel(excel_writer,index=False,sheet_name="MB52")
df_MB52_groupby.to_excel(excel_writer,index=False,sheet_name="MB52_groupby")


excel_writer.save()
print("Status_Cleansing.xlsx save successfully")

output6 = (time.time()-t6)
print('Time taken in seconds extracting df to excel : ' + str(output6))