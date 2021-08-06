from numpy import int64
import pandas as pd
import time
import logging

from datetime import datetime

today_now = datetime.now()
todate = datetime.today().strftime('%Y-%m-%d')

#Relative Path
DATAPATH_RAW="./Raw/"
DATAPATH_DIM="./Dim/"

#Report from ECC
DATAFILE_EORD = "EORD_GAR.XLSX"
DATAFILE_MARC = 'MARC_GAR.xlsx'
DATAFILE_MVKE_3090 = "MVKE_3090.xlsx"

#X-refence / Dimension files
DATAFILE_GAR_PLANTS = 'GAR_Plants.xlsx'
DATAFILE_SPK_Xref = "SPK_Xref.XLSX"
DATAFILE_MG5_Xref = "Apex_MG5.xlsx"


#Create a logging file
logging.basicConfig(filename='./Log/SL_SPK_Cleansing_'+ todate +'.log', 
    encoding='utf-8', 
    level=logging.DEBUG,
    filemode='w',
    format='%(asctime)s %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p')

#Load file
print("Loading Plants/SPK X-Ref file into data frame...")
logging.info("Loading Plants/SPK X-Ref file into data frame...")

t1 = time.time()
df_MARC= pd.read_excel(DATAPATH_RAW + DATAFILE_MARC,engine="openpyxl")
df_MVKE_3090 = pd.read_excel(DATAPATH_RAW + DATAFILE_MVKE_3090,engine="openpyxl")
df_GAR_Plants = pd.read_excel(DATAPATH_DIM + DATAFILE_GAR_PLANTS,engine="openpyxl")
df_SPK_Xref= pd.read_excel(DATAPATH_DIM + DATAFILE_SPK_Xref,engine="openpyxl")
df_MG5_Xref = pd.read_excel(DATAPATH_DIM + DATAFILE_MG5_Xref,engine="openpyxl")

output1 = (time.time()-t1)
print('Time taken in seconds loading files: ' + str(output1))
logging.info('Time taken in seconds loading files: ' + str(output1))

print("Loading EORD file into data frame...")
logging.info('Loading EORD file into data frame... ')

t2 = time.time()
df_EORD_raw = pd.read_excel(DATAPATH_RAW + DATAFILE_EORD,engine="openpyxl")
output2 = (time.time()-t2)
print('Time taken in seconds loading EORD: ' + str(output2))
logging.info('Time taken in seconds loading EORD: ' + str(output2))


#Formatting files: Convert all the integers to string
t3 = time.time()
df_GAR_Plants=df_GAR_Plants.astype({"Plant":'str',"Sales Org":'str',"Vendor":'str'})
df_MARC= df_MARC[df_MARC['Plant-sp.matl status'].notna()]
df_MARC["Plant-sp.matl status"]=df_MARC["Plant-sp.matl status"].apply(int64)
df_MARC=df_MARC.astype({"Plant":'str',"Material":'str',"Plant-sp.matl status":'str'})
df_EORD_raw=df_EORD_raw.astype({"Material":'str',"Plant":'str',"Number":'str',"Vendor":'str'})
df_SPK_Xref=df_SPK_Xref.astype({"Vendor to SPK":'str',"Vendor":'str'})
df_MVKE_3090=df_MVKE_3090.astype({"Material":'str',"Default Plant":'str'})
df_MG5_Xref=df_MG5_Xref.astype({"Apex MG5 to Vendor SPK":'str',"Default Plant":'str'})

#create an unique key Material/Plant/Number in EORD and Material/Plant for SPK vlookup
df_EORD_raw['Material/Plant'] = df_EORD_raw['Material'] +"/" + df_EORD_raw['Plant']
df_EORD_raw['Material/Plant/Number'] = df_EORD_raw['Material'] +"/" + df_EORD_raw['Plant'] +"/" +df_EORD_raw['Number'] 
#create an unique key Material/Plant/Number in MARC
df_MARC['Material/Plant'] = df_MARC['Material'] +"/" + df_MARC['Plant']
df_MARC['Material/IntraCo Procurement Plant'] = df_MARC['Material']+"/"+df_MARC['Plant']

output3 = (time.time()-t3)
print('Time taken in seconds formatting files: ' + str(output3))
logging.info('Time taken in seconds formatting files: ' + str(output3))

#Filter out expired & blocked SL
def get_invalid_SL(df_EORD):
    expired_SL = df_EORD['Valid to'] < today_now
    blocked_SL = df_EORD['Blocked']=='X'

    df_expired_SL = df_EORD[expired_SL]
    df_blocked_SL = df_EORD[blocked_SL]

    return pd.concat([df_expired_SL,df_blocked_SL]).drop_duplicates(subset = 'Material/Plant/Number')

#Filter out plants not supported by GAR team (India & MFG)
def get_plants_OOS(df_EORD,df_plant):
    plants_OOS = df_plant[df_plant['SPK SL Project']=="No"]
    is_plants_OOS = df_EORD['Plant'].isin(plants_OOS['Plant'])
    df_plants_OOS = df_EORD[is_plants_OOS]
    return df_plants_OOS

#Filter out expired & blocked & OOS records to get valid SL
def get_valid_SL(df_EORD,df_invalid_SL,df_plants_OOS):
    df_invalid = pd.concat([df_invalid_SL,df_plants_OOS]).drop_duplicates(subset = 'Material/Plant/Number')
    is_invalid = df_EORD['Material/Plant/Number'].isin(df_invalid['Material/Plant/Number'])
    return df_EORD[~is_invalid]

def map_valid_SL_SPK_Xref(df_valid_SL,df_MARC,df_SPK_Xref,df_GAR_Plants):
    df_valid_SL_map_SPK_Xref = pd.merge(df_valid_SL,df_MARC[['Material/Plant','Plant-sp.matl status','SpecProcurem Costing']],on='Material/Plant',how='left')
    df_valid_SL_map_SPK_Xref = pd.merge(df_valid_SL_map_SPK_Xref,df_SPK_Xref[['Vendor','Vendor to SPK']],on='Vendor',how='left')
    df_valid_SL_map_SPK_Xref = pd.merge(df_valid_SL_map_SPK_Xref,df_GAR_Plants[['Plant','Country']],on='Plant',how='left')
    
    df_valid_SL_map_SPK_Xref.rename(columns={'Plant-sp.matl status':'Plant Status','SpecProcurem Costing':'Plant SPK'},inplace=True)

    df_valid_SL_map_SPK_Xref.loc[df_valid_SL_map_SPK_Xref['Vendor'].str.startswith('1'),'Vendor to SPK']='20'
    df_valid_SL_map_SPK_Xref.loc[df_valid_SL_map_SPK_Xref['Vendor']=='9000341','Vendor to SPK']='Ok,buy from 9000341 (Swissco),FIN review in Year end'
    df_valid_SL_map_SPK_Xref.loc[df_valid_SL_map_SPK_Xref['Vendor']=='9000341','Comment']='Ok,buy from 9000341 (Swissco),FIN review in Year end'

    return df_valid_SL_map_SPK_Xref

def check_valid_SL_SPK_Xref(df_valid_SL_map_SPK_Xref):
    #Identify Dual sources
    #df_dual_SL = df_valid_SL_map_SPK_Xref[df_valid_SL_map_SPK_Xref.duplicated(['Material/Plant'],keep=False)]
    df_valid_SL_map_SPK_Xref.loc[df_valid_SL_map_SPK_Xref.duplicated(['Material/Plant'],keep=False),"Comment"] = "MDO action, check dual sources"

    #Filter out cancelled SKU in plant
    df_valid_SL_map_SPK_Xref.loc[(df_valid_SL_map_SPK_Xref['Plant Status']=='70') | (df_valid_SL_map_SPK_Xref['Plant Status']=='80'),"Comment"]="MDO action,SKU cancelled in Plant,why SL still exist?"

    #Filter out cty buy from Apex 9000340
    df_valid_SL_map_SPK_Xref.loc[(df_valid_SL_map_SPK_Xref['Vendor']=='9000340') & (df_valid_SL_map_SPK_Xref["Comment"].isna()),"Comment"]="Cty Buy via Apex, check sheet 'SKU via Apex SPK' for more details"

    return df_valid_SL_map_SPK_Xref


def get_buy_via_Apex(df_valid_SL_map_SPK_Xref,df_MARC,df_MVKE_3090,df_MG5_Xref):
    #Create new table to store SKU buy via Apex
    df_cty_via_Apex = df_valid_SL_map_SPK_Xref[df_valid_SL_map_SPK_Xref["Comment"]=="Cty Buy via Apex, check sheet 'SKU via Apex SPK' for more details"]

    df_cty_via_Apex["Material/3090"] = df_cty_via_Apex['Material'] + "/3090"

    df_MARC_3090 = df_MARC[df_MARC['Plant']=='3090']
    df_MARC_3090.rename(columns={'Material/Plant':'Material/3090','Plant-sp.matl status':'Plant Status in 3090','SpecProcurem Costing':'SPK in 3090'},inplace=True)


    df_EORD_3090 = df_valid_SL_map_SPK_Xref[df_valid_SL_map_SPK_Xref['Plant']=='3090']
    df_EORD_3090.rename(columns={'Material/Plant':'Material/3090','Vendor':'Vendor in 3090'},inplace=True)

    df_cty_via_Apex_SPK = pd.merge(df_cty_via_Apex,df_MARC_3090[['Material/3090','Plant Status in 3090','SPK in 3090']],on='Material/3090',how='left')
    df_cty_via_Apex_SPK = pd.merge(df_cty_via_Apex_SPK,df_EORD_3090[['Material/3090','Vendor in 3090']],on='Material/3090',how='left')

    #Get MG5 into Apex file
    df_MVKE_3090 = pd.merge(df_MVKE_3090,df_MG5_Xref[['Default Plant','Apex MG5 to Vendor SPK']],on='Default Plant',how='left')
    df_cty_via_Apex_SPK = pd.merge(df_cty_via_Apex_SPK,df_MVKE_3090[['Material','Default Plant','Apex MG5 to Vendor SPK']],on='Material',how='left')
    #print(df_cty_via_Apex_SPK[df_cty_via_Apex_SPK.duplicated(subset=['Material/Plant/Number'],keep=False)])

    return df_cty_via_Apex_SPK

#Comment based on different Apex scenario
def check_buy_via_Apex(df_cty_via_Apex_SPK):
    df_cty_via_Apex_SPK.loc[
        ((df_cty_via_Apex_SPK['Vendor in 3090'].str.startswith('1')) | (df_cty_via_Apex_SPK['Vendor in 3090']=='9000033')) &
        (df_cty_via_Apex_SPK['Plant SPK']=='5S' )&
        (df_cty_via_Apex_SPK['Comment']=="Cty Buy via Apex, check sheet 'SKU via Apex SPK' for more details"),
        'Comment'] = "Ok, Buy via Apex via Temse or 3rd party"

    df_cty_via_Apex_SPK.loc[
        ((df_cty_via_Apex_SPK['Vendor in 3090'].str.startswith('1')) | (df_cty_via_Apex_SPK['Vendor in 3090']=='9000033')) &
        (df_cty_via_Apex_SPK['Plant SPK']!='5S' )&
        (df_cty_via_Apex_SPK['Comment']=="Cty Buy via Apex, check sheet 'SKU via Apex SPK' for more details"),
        'Comment'] = "MDO action, Apex buy from Non-ECC vendor but country SPK is not 5S"

    df_cty_via_Apex_SPK.loc[
        (df_cty_via_Apex_SPK['Comment']=="Cty Buy via Apex, check sheet 'SKU via Apex SPK' for more details") &
        (df_cty_via_Apex_SPK['Vendor in 3090'].isna()) &
        (df_cty_via_Apex_SPK['Default Plant']=='300'),'Comment'] ='MDO action,check with Apex MDO Apex SL missing but MG5=300'

    df_cty_via_Apex_SPK.loc[
        (df_cty_via_Apex_SPK['Plant SPK']==df_cty_via_Apex_SPK['Apex MG5 to Vendor SPK']) &
        (df_cty_via_Apex_SPK['Comment']=="Cty Buy via Apex, check sheet 'SKU via Apex SPK' for more details"), 
        'Comment']="Ok, Cty SPK match with Apex MG5"

    df_cty_via_Apex_SPK.loc[
    (df_cty_via_Apex_SPK['Comment']=="Cty Buy via Apex, check sheet 'SKU via Apex SPK' for more details") &
    (df_cty_via_Apex_SPK['Default Plant'].isna()),'Comment'] ='MDO action,check with Apex MDO as SKU not extended under 3000/20'

    df_cty_via_Apex_SPK.loc[
        (df_cty_via_Apex_SPK['Comment']=="Cty Buy via Apex, check sheet 'SKU via Apex SPK' for more details") &
        (df_cty_via_Apex_SPK['Apex MG5 to Vendor SPK'].isna()),'Comment'] ='MDO action,check with Apex MDO as 3000/20 MG5 not in scope'

    df_cty_via_Apex_SPK.loc[
        (df_cty_via_Apex_SPK['Comment']=="Cty Buy via Apex, check sheet 'SKU via Apex SPK' for more details"),'Comment'] ='MDO action,Cty SPK mistmatch with Apex MG5, pls check with Apex MDO on what is the actual source'

    return df_cty_via_Apex_SPK

#Filter out intra-company purchase in China & Japan (Special SPK model Sub DC SPK = Main DC SPK = End source)
def get_Intraco_CN_JP(df_valid_SL_map_SPK_Xref,df_GAR_Plants):
    df_CN_Plants = df_GAR_Plants[
    (df_GAR_Plants['SPK SL Project']=='Yes') & (df_GAR_Plants['Sales Org'] =='3405')]

    df_JP_Plants = df_GAR_Plants[
        (df_GAR_Plants['SPK SL Project']=='Yes') &  (df_GAR_Plants['Sales Org'] =='3900')]   

    df_Intra_CN = df_valid_SL_map_SPK_Xref[
        (df_valid_SL_map_SPK_Xref['Plant'].isin(df_CN_Plants['Plant'])) &
        (df_valid_SL_map_SPK_Xref['Vendor'].isin(df_CN_Plants['Vendor'])) &
        (df_valid_SL_map_SPK_Xref["Comment"].isna())]

    df_Intra_JP = df_valid_SL_map_SPK_Xref[
        (df_valid_SL_map_SPK_Xref['Plant'].isin(df_JP_Plants['Plant'])) &
        (df_valid_SL_map_SPK_Xref['Vendor'].isin(df_JP_Plants['Vendor'])) &
        (df_valid_SL_map_SPK_Xref["Comment"].isna())]

    df_Intra_CN_JP = pd.concat([df_Intra_CN,df_Intra_JP],ignore_index=True)

    df_Intra_CN_JP['Material/IntraCo Procurement Plant']=df_Intra_CN_JP['Material']+"/"+df_Intra_CN_JP['Vendor'].str[3:]

    df_Intra_CN_JP = pd.merge(df_Intra_CN_JP,df_MARC[['Material/IntraCo Procurement Plant','Plant-sp.matl status','SpecProcurem Costing']],on='Material/IntraCo Procurement Plant',how='left')
    df_Intra_CN_JP.rename(columns={'Plant-sp.matl status':'InterCo Procurement Plant Status','SpecProcurem Costing':'InterCo Procurement Plant SPK'},inplace=True)
    
    return df_Intra_CN_JP

def check_Intraco_CN_JP(df_Intra_CN_JP):
    df_Intra_CN_JP.loc[
    (df_Intra_CN_JP['Plant SPK']==df_Intra_CN_JP['InterCo Procurement Plant SPK']),
    'Comment']="Ok, InterCo Sub Plant SPK match Main Plant SPK"

    df_Intra_CN_JP.loc[
        (df_Intra_CN_JP['Comment'].isna()) &
        (df_Intra_CN_JP['InterCo Procurement Plant Status'].isna()),
        'Comment']="MDO action, InterCo Main Plant obsolete,but Sub plant still active,can discon in Sub Plant?"

    df_Intra_CN_JP.loc[
        (df_Intra_CN_JP['Comment'].isna()),
        'Comment']="MDO action, InterCo Sub plant and Main plant SPK mismatch"

    return df_Intra_CN_JP

#Output dataframe df_EORD_invalid
t4 = time.time()
df_EORD_invalid = get_invalid_SL(df_EORD_raw)
output4 = (time.time()-t4)
print('Time taken in seconds outputting file - EORD invalid SL: ' + str(output4))
logging.info('Time taken in seconds outputting file - EORD invalid SL: ' + str(output4))

#Output dataframe out of scope: df_EORD_plants_OOS
t5 = time.time()
df_EORD_plants_OOS = get_plants_OOS(df_EORD_raw,df_GAR_Plants)
output5 = (time.time()-t5)
print('Time taken in seconds outputting file - EORD Out of scope: ' + str(output5))
logging.info('Time taken in seconds outputting file - EORD Out of scope: ' + str(output5))

#Output dataframe df_valid_SL
t7 = time.time()
df_valid_SL = get_valid_SL(df_EORD_raw,df_EORD_invalid,df_EORD_plants_OOS)
df_valid_SL_map_SPK_Xref_WIP = map_valid_SL_SPK_Xref(df_valid_SL,df_MARC,df_SPK_Xref,df_GAR_Plants)
df_valid_SL_map_SPK_Xref = check_valid_SL_SPK_Xref(df_valid_SL_map_SPK_Xref_WIP)
df_cty_via_Apex_SPK_WIP = get_buy_via_Apex(df_valid_SL_map_SPK_Xref,df_MARC,df_MVKE_3090,df_MG5_Xref)
df_cty_via_Apex_SPK = check_buy_via_Apex(df_cty_via_Apex_SPK_WIP)

#Filter out intra-company purchase in China & Japan (Special SPK model Sub DC SPK = Main DC SPK = End source)
df_Intra_CN_JP_WIP = get_Intraco_CN_JP(df_valid_SL_map_SPK_Xref,df_GAR_Plants)
df_valid_SL_map_SPK_Xref.loc[df_valid_SL_map_SPK_Xref['Material/Plant/Number'].isin(df_Intra_CN_JP_WIP['Material/Plant/Number']),"Comment"] = "CN,JP Intra-company SL, check sheet 'CN_JP_Intra' for more details"
df_Intra_CN_JP = check_Intraco_CN_JP(df_Intra_CN_JP_WIP)

#In Main SL sheet, Check if Cty SPK = Vendor SPK for records with no comments yet
df_valid_SL_map_SPK_Xref.loc[
    (df_valid_SL_map_SPK_Xref['Plant SPK']==df_valid_SL_map_SPK_Xref['Vendor to SPK']) &
    (df_valid_SL_map_SPK_Xref["Comment"].isna()),
    "Comment"]="Ok, SPK match SL"

df_valid_SL_map_SPK_Xref.loc[
    (df_valid_SL_map_SPK_Xref['Plant SPK']=="TW") &
    (df_valid_SL_map_SPK_Xref["Comment"].isna()),
    "Comment"]="Ok, FIN will conduct yearly check for TW"

df_valid_SL_map_SPK_Xref.loc[
    df_valid_SL_map_SPK_Xref["Comment"].isna(),
    "Comment"]="MDO action, SPK mismatch SL"

df_valid_SL_map_SPK_Xref = pd.merge(df_valid_SL_map_SPK_Xref,df_cty_via_Apex_SPK[['Material/Plant/Number','Comment']],on='Material/Plant/Number',how='left')
df_valid_SL_map_SPK_Xref = pd.merge(df_valid_SL_map_SPK_Xref,df_Intra_CN_JP[['Material/Plant/Number','Comment']],on='Material/Plant/Number',how='left')

#Combine Comments into 1 "Analysis" Column, axis = 1 to get value from row
df_valid_SL_map_SPK_Xref['Analysis'] = df_valid_SL_map_SPK_Xref.apply(
    lambda df: df['Comment_y'] if df['Comment_x']=="Cty Buy via Apex, check sheet 'SKU via Apex SPK' for more details" else df['Comment_x'],axis=1)

df_valid_SL_map_SPK_Xref['Analysis'] = df_valid_SL_map_SPK_Xref.apply(
    lambda df: df['Comment'] if df['Comment_x']=="CN,JP Intra-company SL, check sheet 'CN_JP_Intra' for more details" else df['Analysis'],axis=1)

#Drop Comment columns from Apex & CN_JP_intra worksheets since 'Analysis' Column exists
df_valid_SL_map_SPK_Xref.drop(['Comment_y','Comment'],axis=1,inplace=True)

#Rename Column "Comment_x" to "Comment" and move it to the end of the columns
df_valid_SL_map_SPK_Xref.rename(columns = {'Comment_x':'Comment'},inplace=True)

output7 = (time.time()-t7)
print('Time taken in seconds outputting file - EORD valid_SL ' + str(output7))
logging.info('Time taken in seconds outputting file - EORD valid_SL ' + str(output7))

#Extract into Excel
t6 = time.time()

excel_writer = pd.ExcelWriter('./' + "SPK_SL_Cleansing." + "xlsx", engine = 'xlsxwriter')
#df_EORD_invalid.to_excel(excel_writer,index = False, sheet_name = 'Invalid SL')
#df_GAR_Plants.to_excel(excel_writer,index = False, sheet_name = 'GAR Plants')
#df_EORD_plants_OOS.to_excel(excel_writer,index = False, sheet_name = 'GAR Plants OOS')
#df_valid_SL.to_excel(excel_writer,index = False, sheet_name = 'Valid SL')
df_valid_SL_map_SPK_Xref.to_excel(excel_writer,index = False, sheet_name = 'Valid SL with SPK')
#df_cty_via_Apex.to_excel(excel_writer,index = False, sheet_name = 'SKU via APEX')
df_Intra_CN_JP.to_excel(excel_writer,index = False, sheet_name = 'CN_JP_Intra')
df_cty_via_Apex_SPK.to_excel(excel_writer,index = False, sheet_name = 'SKU via APEX SPK')
excel_writer.save()
#print("SPK_SL_Output.xlsx save successfully")

output6 = (time.time()-t6)
print('Time taken in seconds extracting df to excel : ' + str(output6))
logging.info('SPK_SL_Output.xlsx save successfully. /n Time taken in seconds extracting data to excel : ' + str(output6))
total_time_spent = output1+output2+output3+output4+output5+output6
print('Time taken in seconds extracting df to excel : ' + str(total_time_spent))
logging.info('Total time spent in completing the task : ' + str(total_time_spent))

