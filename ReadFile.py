#!/usr/bin/env python3
import json
import datetime
import mysql.connector
import sys
import urllib.request
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
        json_array[row][1] = EnergyReal_WAC_Plus_Absolute[key]
        json_array[row][2] = EnergyReal_WAC_Minus_Absolute[key]
        json_array[row][3] = EnergyReal_WAC_Sum_Produced[key]

    return json_array
    
def openDBconnection():
    try:
          cnx = mysql.connector.connect(host='10.0.1.10',
                                        user='PV_user',
                                        password='LEmUGR3vewIWcKrU',
                                        database='PV')
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

  return record[1]

# ============
#     Start
# ============

#startDate = sys.argv[1]
startDate = getLastDate()
print (startDate)

url = "http://10.0.1.58/solar_api/v1/GetArchiveData.cgi?Scope=System&StartDate=" + startDate + "&EndDate=" + startDate + "&Channel=EnergyReal_WAC_Sum_Produced&Channel=TimeSpanInSec&Channel=EnergyReal_WAC_Plus_Absolute&Channel=EnergyReal_WAC_Minus_Absolute"

data = json.loads(urllib.request.urlopen(url).read().decode('utf-8'))

StartDate = datetime.datetime.strptime(data["Body"]["Data"]["meter:16501544"]["Start"], "%Y-%m-%dT%H:%M:%S+01:00")
dataArray = ConvertJSON(data)

data_dict = ConvertJSON(data)
print(data_dict)
# con = openDBconnection()
# cursor = con.cursor()
# row_count = 0

# for row in data_dict:
    
#     secOffset = row[0] + 3600
#     timestamp = StartDate + datetime.timedelta(seconds=secOffset)
    
#     sql = "INSERT INTO T_PowerLog (DateTime, EnergyReal_WAC_Sum_Produced, EnergyReal_WAC_Plus_Absolute, EnergyReal_WAC_Minus_Absolute) VALUES (%s, %s, %s, %s)"
#     values = (timestamp, row[3], row[1], row[2]) 
#     cursor.execute(sql, values)
#     recordID = cursor.lastrowid
#     row_count += 1
    
# con.commit()
# cursor.close()
# con.close()
    
#print(str(row_count) + " records written")
