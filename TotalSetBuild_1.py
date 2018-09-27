
# -*- coding: utf-8 -*-

#https://github.com/bbb-fish/Spectrum.git

"""
Created on Thurs September 27th
@author: P2813634
"""
###
from multiprocessing import Pool
import pandas as pd
import numpy as np
import os, fnmatch
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from datetime import date, timedelta
from sklearn.ensemble import IsolationForest
from sklearn import preprocessing

#Constants
MINTRX = 100
PROCESSORS = 8
CHTR_DAYS=6

dir = '/nf/home/pxrao/GIT/AnomalyDetection'
yesterday = date.today() - timedelta(1)
currDate = date.today()
yyyymmdd = currDate.strftime('%Y%m%d')
fromaddr = "praveen.rao@charter.com"
#toaddr = ["praveen.rao@charter.com","robert.a.fisher@charter.com","C-Anshumam.Deshmukh@charter.com","C-Mradul.Dhakad@charter.com","C-Jaspreet.Kaur@charter.com","Steve.Ahlbrand@charter.com"]
#toaddr = ["praveen.rao@charter.com"]
toaddr = ["robert.a.fisher@charter.com"]

print("Current date: " + yyyymmdd)

#Read Non-CHTR files from directory and create df
fname = 'trx_'+yyyymmdd+'.csv.gz'

df = pd.DataFrame()
for filename in fnmatch.filter(os.listdir(dir), fname):
    print("File name: " + filename, flush=True)
    df = df.append(pd.read_csv(dir + '/' + filename, parse_dates = ['TUNING_EVNT_START_DT']))

#Sort values by date and set up to_integer function
df = df.sort_values(by = ['TUNING_EVNT_START_DT'])
def to_integer(dt):
    return 10000*dt.dt.year + 100*dt.dt.month + dt.dt.day

tt0 = time.time()

#Loop through dataframe and perform analysis
##

def iforest(df_masdiv):

    t0 = time.time()
    df_total = pd.DataFrame()
    print(np.unique(df_masdiv.MASDIV))
    for STATION in np.unique(df_masdiv.STATION):
        df2 = df_masdiv.copy().loc[df_masdiv['STATION'] == STATION]
        if len(df2) > 0:
            ## Break out if avg trx < MINTRX
            if (np.mean(df2['TRANSACTIONS']) >= MINTRX) :
            ##Label encoding
              le = preprocessing.LabelEncoder()
              le.fit(df2["DOW"])
              df2["DOW_INT"] = le.transform(df2["DOW"])
              le.fit(df2["MOY"])
              df2["MOY_INT"] = le.transform(df2["MOY"])
              df2['DT_NBR'] = to_integer(df2.TUNING_EVNT_START_DT)


            ##Fill in missing dates
              df2['TUNING_EVNT_START_DT'] = pd.to_datetime(df2['TUNING_EVNT_START_DT'])
              dates = df2.set_index('TUNING_EVNT_START_DT').resample('D').asfreq().index
              masdiv = df2['MASDIV'].unique()
              station = df2['STATION'].unique()
              idx = pd.MultiIndex.from_product((dates, masdiv, station), names=['TUNING_EVNT_START_DT', 'MASDIV', 'STATION'])
              df2 = df2.set_index(['TUNING_EVNT_START_DT', 'MASDIV', 'STATION']).reindex(idx, fill_value=0).reset_index()

            ##Set up data for iForest
              df2_save = df2
              df2_select = df2.drop(['MASDIV','STATION','DOW','MOY','TUNING_EVNT_START_DT'], 1)

            ##Run through iForest
              clf = IsolationForest(n_estimators=100, max_samples="auto", contamination = 0.01)
              clf.fit(df2_select)
              scores_pred = clf.decision_function(df2_select)
              y_pred_test = clf.predict(df2_select)
              df2_save['scores'] = scores_pred
              df2_save['anomaly'] = y_pred_test

            ##Assign anomalies based on anomaly score
              df2_save['t_anomaly'] = np.where(df2_save['scores'] < -0.2, -1, 1)
              df2_save = df2_save.sort_values(by = ['DT_NBR'])

            ##Add results from loop to final df
              df_total = df_total.append(df2_save, ignore_index=True)

    t1 = time.time()
    print("Elapsed time: ", "%.2f" % ((t1-t0)/60), " mins", flush=True)
    return(df_total)

##

#Set up multiprocessing and run script
pool = Pool(processes=PROCESSORS)
args = np.unique(df.MASDIV).tolist()
print("Number of MASDIVs: %d" % len(args), flush=True)
x = pool.map(iforest, (df.loc[df['MASDIV'] == masdiv] for masdiv in args))
tt1 = time.time()
print("Total Elapsed time: ", "%.2f" % ((tt1-tt0)/60), " mins", flush=True)
df_total = pd.concat(x)

#Building Superset for Teradata
SuperSet = df_total
SuperSet.to_csv("SuperSet_" + yyyymmdd + ".csv")
fname3 = "SuperSet_" + yyyymmdd + ".csv"
print("SuperSet created as a CSV")

### Here's where some SQL will go

#Finalize df
result_anomaly = df_total.loc[df_total['t_anomaly'] == -1]
max_date = max(df_total['TUNING_EVNT_START_DT'])

#Export results to csv files
result_anomaly_2 = result_anomaly.loc[result_anomaly['TUNING_EVNT_START_DT'] >= currDate-timedelta(CHTR_DAYS)]
result_anomaly_2 = result_anomaly_2.loc[result_anomaly_2['MASDIV'] != 'Charter']

result_anomaly_2.to_csv("anomaly_nonchtr_" + (currDate-timedelta(CHTR_DAYS)).strftime('%Y%m%d') + ".csv")
df_total.to_csv("data_nonchtr_" + (currDate-timedelta(CHTR_DAYS)).strftime('%Y%m%d') + ".csv")

#result_anomaly_2.to_csv("anomaly_maxdate_" + max_date.strftime('%Y%m%d') + ".csv")
#df_total.to_csv("data_" + max_date.strftime('%Y%m%d') + ".csv")


#################
#Repeat entire process specifically for CHTR MASDIV (updated later)

#Read CHTR files from directory and create df
fname = 'trx_chtr_'+yyyymmdd+'.csv.gz'
df = pd.DataFrame()
for filename in fnmatch.filter(os.listdir(dir), fname):
    print("File name: " + filename, flush=True)
    df = df.append(pd.read_csv(dir + '/' + filename, parse_dates = ['TUNING_EVNT_START_DT']))

#Sort df by date
df = df.sort_values(by = ['TUNING_EVNT_START_DT'])

#Set up multiprocessing and run script
pool = Pool(processes=PROCESSORS)
args = np.unique(df.MASDIV).tolist()
print("Number of MASDIVs: %d" % len(args), flush=True)
x = pool.map(iforest, (df.loc[df['MASDIV'] == masdiv] for masdiv in args))
tt1 = time.time()
print("Total Elapsed time: ", "%.2f" % ((tt1-tt0)/60), " mins", flush=True)
df_total = pd.concat(x)

#Finalize df
result_anomaly_1 = df_total.loc[df_total['t_anomaly'] == -1]
#max_date = max(df_total['TUNING_EVNT_START_DT'])

#Export results to csv files
result_anomaly_CHTR = result_anomaly_1.loc[result_anomaly_1['TUNING_EVNT_START_DT'] >= currDate-timedelta(CHTR_DAYS)]
#result_anomaly_CHTR = result_anomaly_CHTR.loc[result_anomaly_CHTR['MASDIV'] == 'Charter']
result_anomaly_CHTR.to_csv("anomaly_chtr_" + (currDate-timedelta(CHTR_DAYS)).strftime('%Y%m%d') + ".csv")
df_total.to_csv("data_chtr_" + (currDate-timedelta(CHTR_DAYS)).strftime('%Y%m%d') + ".csv")
###

##################
#Email results
#fname1 = "anomaly_chtr_" + max_date.strftime('%Y%m%d') + ".csv"
fname1 = "anomaly_nonchtr_" + (currDate-timedelta(CHTR_DAYS)).strftime('%Y%m%d') + ".csv"
fname2 = "anomaly_chtr_" + (currDate-timedelta(CHTR_DAYS)).strftime('%Y%m%d') + ".csv"
msg = MIMEMultipart()
msg['From'] = fromaddr
msg['To'] = ", ".join(toaddr)


if result_anomaly_1.dropna().empty & result_anomaly_CHTR.dropna().empty:

    msg['Subject'] = "No anomalies"
    body = "No anomalies found"
    msg.attach(MIMEText(body, 'plain'))
    text = msg.as_string()
    Otherfilename = fname3
    attachment = open(Otherfilename, "rb")

elif result_anomaly_1.dropna().empty:
    msg['Subject'] = "Anomalies " + yyyymmdd
    body = "File attached"
    msg.attach(MIMEText(body, 'plain'))
    filename = fname2
    attachment = open(filename, "rb")
    #Adding some attachments
    Otherfilename = fname3
    attachment = open(Otherfilename, "rb")

    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', "attachment; filename= %s" % filename)

    msg.attach(part)
    text = msg.as_string()

elif result_anomaly_CHTR.dropna().empty:
    msg['Subject'] = "Anomalies " + yyyymmdd
    body = "File attached"
    msg.attach(MIMEText(body, 'plain'))
    filename = fname1
    attachment = open(filename, "rb")
    #Adding some attachments
    Otherfilename = fname3
    attachment = open(Otherfilename, "rb")

    part = MIMEBase('application', 'octet-stream')
    part.set_payload((attachment).read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition', "attachment; filename= %s" % filename)

    msg.attach(part)
    text = msg.as_string()

else:
    msg['Subject'] = "Anomalies " + yyyymmdd
    body = "File attached"
    msg.attach(MIMEText(body, 'plain'))
    filename = [fname1, fname2]
    for fname in filename:
        attachment = open(fname, "rb")
        part = MIMEBase('application', 'octet-stream')
        part.set_payload((attachment).read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', "attachment; filename= %s" % fname)
        msg.attach(part)
    text = msg.as_string()

server = smtplib.SMTP('localhost')
server.sendmail(fromaddr, toaddr, text)
server.quit()
print("Successfully sent email")
