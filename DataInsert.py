import pandas as pd
import pyodbc
import datetime

#READ IN CSV TEST DATA
df = pd.read_csv('C:\\Users\\p2813634\\Desktop\\Work\\Anomaly_Detection\\Data\\anomaly_nonchtr_20181004.csv')
print('CSV LOADED')
count_rows = df.shape[0]
print(count_rows)

###############################################
## Data Type Manipulations to match Teradata ##
###############################################

#DATE ALGORITHM IS RUN AND RECORDS ARE SCORED
#RUN_DT = datetime.date(2018, 10, 4) #year, month, today
RUN_DT = datetime.datetime.today().strftime('%Y-%m-%d')
print(RUN_DT)

#DATE RECORD ADDED TO TERADATA TABLE
#CREATED_DT = datetime.date(2018, 8, 29) #year, month, today
CREATED_DT = datetime.datetime.today().strftime('%Y-%m-%d')

#ADJUST DATE FORMAT ON TUNING_EVNT_START_DT
df['TUNING_EVNT_START_DT'] = pd.to_datetime(df.TUNING_EVNT_START_DT)
df['TUNING_EVNT_START_DT'] = df['TUNING_EVNT_START_DT'].dt.strftime('%Y-%m-%d')

#SET TRANSACTION TYPE
#TRXTYPE = 'chtr'
TRXTYPE = 'nonchtr'
#print(TRXTYPE)

#CHANGE SCORES TO FLOAT
df['scores'] = df['scores'].astype(float)

#CHANGE ANOMALY COLUMNS TO INTO
df['anomaly'] = df['anomaly'].astype(int)
df['t_anomaly'] = df['t_anomaly'].astype(int)

######################
## PUSH TO DATABASE ##
######################

conn = pyodbc.connect('dsn=ConnectR')
cursor = conn.cursor()

# Database table has columns...
# PK | TRXTYPE | CREATED_DT | RUN_DT | MASDIV | STATION | TUNING_EVNT_START_DT | DOW | MOY | TRANSACTIONS | SCORES | ANOMALY | T_ANOMALY | ANOMALY_FLAG
# PK is autoincrementing, TRXTYPE needs to be specified on insert command, and ANOMALY_FLAG defaults to 1 for yes

for index, row in df.iterrows():
        cursor.execute("INSERT INTO DLABBUAnalytics_Lab.Anomaly_Detection_SuperSet(TRXTYPE,CREATED_DT,RUN_DT,MASDIV,STATION,TUNING_EVNT_START_DT,DOW,MOY,TRANSACTIONS,SCORES,ANOMALY,T_ANOMALY)VALUES(?,?,?,?,?,?,?,?,?,?,?,?)", TRXTYPE,CREATED_DT,RUN_DT,row['MASDIV'],row['STATION'],row['TUNING_EVNT_START_DT'],row['DOW'],row['MOY'],row['TRANSACTIONS'],row['scores'],row['anomaly'],row['t_anomaly'])
        conn.commit()
        print('RECORD ENTERED')

print('DF SUCCESSFULLY WRITTEN TO DB')
