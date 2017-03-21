#!/usr/bin/env python3
import json
import datetime
import mysql.connector
import sys
import urllib.request
import configparser
#from array import array

def ConvertJSON(json_string):

    TimeSpanInSec = json_string["Body"]["Data"]["inverter/1"]["Data"]["TimeSpanInSec"]["Values"]
    EnergyReal_WAC_Sum_Produced = json_string["Body"]["Data"]["inverter/1"]["Data"]["EnergyReal_WAC_Sum_Produced"]["Values"]
    EnergyReal_WAC_Plus_Absolute = json_string["Body"]["Data"]["meter:16501544"]["Data"]["EnergyReal_WAC_Plus_Absolute"]["Values"]
    EnergyReal_WAC_Minus_Absolute = json_string["Body"]["Data"]["meter:16501544"]["Data"]["EnergyReal_WAC_Minus_Absolute"]["Values"]
 
    keys = list(TimeSpanInSec)
 
    json_array = [[0 for j in range(5)] for i in range(len(TimeSpanInSec))]

    for key in keys:
        row = keys.index(key)
        json_array[row][0] = int(key)
        json_array[row][3] = EnergyReal_WAC_Sum_Produced[key]
        if key in EnergyReal_WAC_Plus_Absolute:
           json_array[row][1] = EnergyReal_WAC_Plus_Absolute[key]
           json_array[row][2] = EnergyReal_WAC_Minus_Absolute[key]
        elif str(int(key)+1) in EnergyReal_WAC_Plus_Absolute:
           key = str(int(key)+1)
           json_array[row][1] = EnergyReal_WAC_Plus_Absolute[key]
           json_array[row][2] = EnergyReal_WAC_Minus_Absolute[key]
        elif str(int(key)-1) in EnergyReal_WAC_Plus_Absolute:
           key = str(int(key)-1)
           json_array[row][1] = EnergyReal_WAC_Plus_Absolute[key]
           json_array[row][2] = EnergyReal_WAC_Plus_Absolute[key]

    return json_array
    
def openDBconnection():
    try:
          cnx = mysql.connector.connect(host=sql_host,
                                        user=sql_user,
                                        password=sql_user_pwd,
                                        database=sql_db)
    except mysql.connector.Error as err:
 #       if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
 #           print("Something is wrong with your user name or password")
 #       elif err.errno == errorcode.ER_BAD_DB_ERROR:
 #           print("Database does not exists")
 #       else:
            print(err)

 #       return null
    else:
        return cnx
    
def getLastDate():
# Get next entry from the queue

  con = openDBconnection()
  cursor = con.cursor()
  cursor.execute("SELECT max(DateTime) from T_PowerLog")

  record = cursor.fetchone()

  con.close()

  return record[0]

# ============
#     Start
# ============

# Read configuration file
config = configparser.ConfigParser()
config.sections()
config.read("PV-Logger.conf")

inverter_ip = config['inverter']['inverter_ip']
sql_host = config['mysql']['dbhost']
sql_db = config['mysql']['db']
sql_user = config['mysql']['dbuser']
sql_user_pwd = config['mysql']['dbpwd']
# ----------------------------------------------------

startDate = (getLastDate() + datetime.timedelta(minutes=1))
endDate = datetime.datetime.now()

url = "http://" + inverter_ip + "/solar_api/v1/GetArchiveData.cgi?Scope=System&StartDate=" + str(startDate.isoformat()) + "+01:00&EndDate=" + str(endDate.isoformat()) + "+01:00&Channel=EnergyReal_WAC_Sum_Produced&Channel=TimeSpanInSec&Channel=EnergyReal_WAC_Plus_Absolute&Channel=EnergyReal_WAC_Minus_Absolute"
data = json.loads(urllib.request.urlopen(url).read().decode('utf-8'))
dataArray = ConvertJSON(data)

data_dict = ConvertJSON(data)

con = openDBconnection()
cursor = con.cursor()
row_count = 0
firstTime = endDate
lastTime = startDate

for row in data_dict:
    
    secOffset = row[0] + 3600
    timestamp = startDate + datetime.timedelta(seconds=secOffset)
    
    sql = "INSERT INTO T_PowerLog (DateTime, EnergyReal_WAC_Sum_Produced, EnergyReal_WAC_Plus_Absolute, EnergyReal_WAC_Minus_Absolute) VALUES (%s, %s, %s, %s)"
    values = (timestamp, row[3], row[1], row[2]) 
#    print (str(values))
    cursor.execute(sql, values)
    recordID = cursor.lastrowid
    row_count += 1
    if timestamp < firstTime:
        firstTime = timestamp
    if timestamp > lastTime:
        lastTime = timestamp
    
con.commit()
cursor.close()
con.close()

# Write LOG-File
logfile = open('PV-Logger.log', "a")
logfile.write("%s: URL: %s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), url))
logfile.write("%s: First entry from: %s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(firstTime)))
logfile.write("%s: Last Entry from: %s\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(lastTime)))
logfile.write("%s: %s records written\n" % (datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), str(row_count)))
logfile.write("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++\n")
logfile.close
