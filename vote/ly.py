#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys # 正常模組
sys.path.append('../')
import re
import codecs
import psycopg2
from datetime import datetime
import db_ly
import ly_common

def FileLog(sitting):
    c.execute('''INSERT into legislator_filelog(sitting,date)
        SELECT %s,%s
        WHERE NOT EXISTS (SELECT 1 FROM legislator_filelog WHERE sitting = %s) RETURNING id''',(sitting,datetime.now(),sitting))
def AddAttendanceRecord(legislator_id,sitting_id,status):
    c.execute('''INSERT into legislator_attendance(legislator_id, sitting_id, category, status)
        SELECT %s, %s, 0, %s
        WHERE NOT EXISTS (SELECT 1 FROM legislator_attendance WHERE legislator_id = %s AND sitting_id = %s)''',(legislator_id, sitting_id, status, legislator_id, sitting_id))
def FindName(c, text, sitting_dict, beginStr, endStr):
    begin = text.find(beginStr)
    end = text.find(endStr)
    if begin == -1 or end == -1:
        return
    sitting_dict.update({"date": ly_common.GetDate(text)})
    ly_common.InsertSitting(c, sitting_dict)
    nameList = text[begin+5:end].split()
    firstName = ''
    for e in nameList:
        #--> 兩個字的立委中文名字中間有空白
        if len(e)<2 and firstName=='':
            firstName = e
            continue
        if len(e)<2 and firstName!='':
            e = firstName + e
            firstName = ''
        #<--
        legislator_id = ly_common.GetLegislatorId(c, e)
        if beginStr == u"出席委員":
            AddAttendanceRecord(legislator_id, sitting_dict["uid"], 'present')
        if beginStr == u"請假委員":
            AddAttendanceRecord(legislator_id, sitting_dict["uid"], 'absent')
    return            
conn = db_ly.con()
c = conn.cursor()
sourcetext = codecs.open(u"立院議事錄08.txt", "r", "utf-8").read()
ms ,me, uid = ly_common.GetSessionROI(sourcetext)
while ms:
    sitting_dict = {"uid":uid, "name": ms.group(1), "ad": ms.group('ad'), "session": ms.group('session') }
    print ms.group(1) 
    FileLog(ms.group(1))
    if me:
        singleSessionText = sourcetext[:me.start()+1]
    else: # last session
        singleSessionText = sourcetext                 
    FindName(c, singleSessionText, sitting_dict, u"出席委員", u"委員出席")
    FindName(c, singleSessionText, sitting_dict, u"請假委員", u"委員請假")
    if me:
        sourcetext = sourcetext[me.start()+1:]
        ms ,me, uid = ly_common.GetSessionROI(sourcetext)
    else: # to the end
        break
conn.commit()
print 'Succeed'
