#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys # 正常模組
sys.path.append('../')
import re,codecs,psycopg2
import db_ly,ly_common
from datetime import datetime

def FileLog(session):
    c.execute('''INSERT into legislator_filelog(session,date)
        SELECT %s,%s
        WHERE NOT EXISTS (SELECT session FROM legislator_filelog WHERE session = %s) RETURNING id''',(session,datetime.now(),session))
def GetLegislatorId(name):
    name_like = name + '%'
    c.execute('''SELECT uid FROM legislator_legislator WHERE name like %s''',[name_like])
    return c.fetchone()[0]
def AddAttendanceRecord(LegislatorID,date,ad,session,sitting,PresentNum,UnpresentNum):
    c.execute('''INSERT into legislator_attendance(legislator_id,date,ad,session,sitting,category,"presentNum","unpresentNum")
        SELECT %s,%s,%s,%s,%s,0,%s,%s
        WHERE NOT EXISTS (SELECT 1 FROM legislator_attendance WHERE legislator_id = %s AND sitting = %s)''',(LegislatorID,date,ad,session,sitting,PresentNum,UnpresentNum,LegislatorID,sitting))
def FindName(text,ad,session,sitting,beginStr,endStr):
    date = ly_common.GetDate(text)
    begin = text.find(beginStr)
    end = text.find(endStr)
    if begin == -1 or end == -1:
        return
    nameList = text[begin+5:end].split()
    firstName = ''
    for e in nameList:
        # 兩個字的立委中文名字中間有空白
        if len(e)<2 and firstName=='':
            firstName = e
            continue
        if len(e)<2 and firstName!='':
            e = firstName + e
            firstName = ''
        #
        LegislatorID = GetLegislatorId(e)
        if beginStr == u"出席委員":
            AddAttendanceRecord(LegislatorID,date,ad,session,sitting,1,0)
        if beginStr == u"請假委員":
            AddAttendanceRecord(LegislatorID,date,ad,session,sitting,0,1)
    return            
conn = db_ly.con()
c = conn.cursor()
sourcetext = codecs.open(u"立院議事錄08.txt", "r", "utf-8").read()
ms , me = ly_common.GetSessionROI(sourcetext)
while ms:
    session = ms.group()
    print session 
    FileLog(session)
    if me:
        singleSessionText = sourcetext[:me.start()+1]
    else:
        singleSessionText = sourcetext                 
    FindName(singleSessionText,ms.group('ad'),ms.group('session'),ms.group(1),u"出席委員",u"委員出席")
    FindName(singleSessionText,ms.group('ad'),ms.group('session'),ms.group(1),u"請假委員",u"委員請假")
    if me:
        sourcetext = sourcetext[me.start()+1:]
        ms , me = ly_common.GetSessionROI(sourcetext)
    else:
        break
conn.commit()
print 'Succeed'


