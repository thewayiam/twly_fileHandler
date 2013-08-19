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
def MakeLegislatorList(name,session):
    c.execute('''INSERT into legislator_legislator(name,enable,"enableSession",hits)
        SELECT %s,true,%s,%s
        WHERE NOT EXISTS (SELECT name,enable,"enableSession",hits FROM legislator_legislator WHERE name = %s)''',(name,session,0,name))
    c.execute('''SELECT id FROM legislator_legislator WHERE name = %s''',[name])
    return c.fetchone()[0]
def AddAttendanceRecord(LegislatorID,date,sessionPrd,session,PresentNum,UnpresentNum):
    c.execute('''INSERT into legislator_attendance(legislator_id,date,"sessionPrd",session,category,"presentNum","unpresentNum")
        SELECT %s,%s,%s,%s,0,%s,%s
        WHERE NOT EXISTS (SELECT legislator_id,date,"sessionPrd",session,category,"presentNum","unpresentNum" FROM legislator_attendance WHERE legislator_id = %s AND session = %s)''',(LegislatorID,date,sessionPrd,session,PresentNum,UnpresentNum,LegislatorID,session))  
def FindName(text,sessionPrd,session,beginStr,endStr):
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
        LegislatorID = MakeLegislatorList(e,session)
        if beginStr == u"出席委員":
            AddAttendanceRecord(LegislatorID,date,sessionPrd,session,1,0)
        if beginStr == u"請假委員":
            AddAttendanceRecord(LegislatorID,date,sessionPrd,session,0,1)
    return            
conn = db_ly.con()
c = conn.cursor()
sourcetext = codecs.open(u"立院議事錄08_01_03.txt", "r", "utf-8").read()
ms , me = ly_common.GetSessionROI(sourcetext)
while ms:
    session = ms.group()
    print session 
    FileLog(session)
    if me:
        singleSessionText = sourcetext[:me.start()+1]
    else:
        singleSessionText = sourcetext                 
    FindName(singleSessionText,ms.group(1),session,u"出席委員",u"委員出席")
    FindName(singleSessionText,ms.group(1),session,u"請假委員",u"委員請假")
    if me:
        sourcetext = sourcetext[me.start()+1:]
        ms , me = ly_common.GetSessionROI(sourcetext)
    else:
        break
conn.commit()
print 'Succeed'


