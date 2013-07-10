#! /usr/bin/env python
# -*- coding: utf-8 -*-
import re
import codecs
import psycopg2
from datetime import datetime

def FileLog(session):
    c.execute('''INSERT into filelog(session,date)
        SELECT %s,%s
        WHERE NOT EXISTS (SELECT session FROM filelog WHERE session = %s) RETURNING id''',(session,datetime.now(),session))
    r = c.fetchone()
    if r:
        return r[0]
def GetSessionROI(text):
    ms , me = re.search(u'立法院第(\d){1,2}屆第[\d]{1,2}會期第[\d]{1,2}次(臨時會第\d次)?會議議事錄',text) , None
    if ms:
        me = re.search(u'立法院第(\d){1,2}屆第[\d]{1,2}會期第[\d]{1,2}次(臨時會第\d次)?會議議事錄',text[1:])     
    return ms , me
def MakeLegislatorList(name,session):
    c.execute('''INSERT into legislator(name,enable,"enableSession")
        SELECT %s,true,%s
        WHERE NOT EXISTS (SELECT name,enable,"enableSession" FROM legislator WHERE name = %s)''',(name,session,name))
    c.execute('''SELECT id FROM legislator WHERE name = %s''',[name])
    return c.fetchone()[0]
def AddAttendanceRecord(LegislatorID,date,sessionPrd,session,PresentNum,UnpresentNum):
    c.execute('''INSERT into attendance(id,date,"sessionPrd",session,category,"presentNum","unpresentNum")
        SELECT %s,%s,%s,%s,0,%s,%s
        WHERE NOT EXISTS (SELECT id,date,"sessionPrd",session,category,"presentNum","unpresentNum" FROM attendance WHERE id = %s AND session = %s)''',(LegislatorID,date,sessionPrd,session,PresentNum,UnpresentNum,LegislatorID,session))  
def GetDate(text):
    matchTerm = re.search(u'(\d)+年(\d)+月(\d)+',text)
    if not matchTerm:
        return None
    matchDate = re.sub('[^0-9]', ' ', matchTerm.group())
    if matchDate:
        dateList = matchDate.split()
        sessionDate = str(int(dateList[0])+1911)
        for e in dateList[1:]:
            if len(e) == 1:
                sessionDate = sessionDate + '-0' + e
            else:
                sessionDate = sessionDate + '-' + e
    else:
        sessionDate = None              
    return sessionDate
def FindName(text,sessionPrd,session,beginStr,endStr):
    date = GetDate(text)
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
conn = psycopg2.connect(dbname='dbname', host='ip', user='user' , password='password')
c = conn.cursor()
sourcetext = codecs.open(u"立院議事錄08_01_03.txt", "r", "utf-8").read()
ms , me = GetSessionROI(sourcetext)
while ms:
    if me:
        singleSessionText = sourcetext[:me.start()+1]
    else:
        singleSessionText = sourcetext
        session = ms.group()
        FileLog(session)
        print session       
        FindName(singleSessionText,ms.group(1),session,u"出席委員",u"委員出席")
        FindName(singleSessionText,ms.group(1),session,u"請假委員",u"委員請假")
        break
    session = ms.group()
    FileLog(session)
    print session       
    FindName(singleSessionText,ms.group(1),session,u"出席委員",u"委員出席")
    FindName(singleSessionText,ms.group(1),session,u"請假委員",u"委員請假")  
    sourcetext = sourcetext[me.start()+1:]
    ms , me = GetSessionROI(sourcetext)
conn.commit()

print u'ly_attendrecord Succeed'

def GetdistrictDetail(text,eleDistrict,name):
    ms, me = re.search(eleDistrict,text) , re.search(name,text)
    if ms and me:
        return text[ ms.end(): me.start()].strip()
def GetSessionEndPoint(text):
    return text.find(u"電話：")
def LyID(name):
    c.execute('''SELECT id FROM legislator WHERE name = %s''',[name])
    return c.rowcount,c.fetchone()
def LyDetail(lyid,eleDistrict,district,party,districtDetail):
    c.execute('''UPDATE legislator SET "eleDistrict" = %s, district = %s, party = %s, "districtDetail" = %s WHERE id = %s''',(eleDistrict,district,party,districtDetail,lyid))
def LiterateLY(text,eleDistrict,district,party,sourcetext2):
    firstName = ''
    for name in text.split():      
        rowcount,lyid = LyID(name)
        if rowcount == 1 and lyid:
            districtDetail = GetdistrictDetail(sourcetext2,eleDistrict,name)
            LyDetail(lyid[0],eleDistrict,district,party,districtDetail)
            break
sourcetext = codecs.open(u"08立委個資.txt", "r", "utf-8").read()
sourcetext2 = codecs.open(u"08立委選區範圍.txt", "r", "utf-8").read()
endP = GetSessionEndPoint(sourcetext)
while(endP != -1):
    singleSessionText = sourcetext[:endP]
    eleDistrict = singleSessionText.rstrip().split()[-1]
    party = singleSessionText.rstrip().split()[-2]
    m = re.search(u'([\W]{2})(縣|市)',eleDistrict)
    if m:
        district = m.group(1)+m.group(2)
    else:
        district = eleDistrict
    #print district
    LiterateLY(singleSessionText,eleDistrict,district,party,sourcetext2)
    sourcetext = sourcetext[endP+1:]
    endP = GetSessionEndPoint(sourcetext)
conn.commit()

def GetSessionEndPoint(text):
    return re.search(u'([\S]{2,9})委員會',text), re.search(u'委員名單',text)
def LyCommittee(lyid,committee):
    c.execute('''UPDATE legislator SET committee = %s WHERE id = %s''',(committee,lyid))
def LiterateCommitter(text,committee):
    for name in text.split():      
        rowcount,lyid = LyID(name)
        if rowcount == 1 and lyid:
            LyCommittee(lyid[0],committee)
        else:
            break
text = codecs.open(u"0803立委委員會.txt", "r", "utf-8").read()
title , start = GetSessionEndPoint(text)
while(title and start):
    committee = title.group(1)
    text = text[start.end():]
    LiterateCommitter(text,committee)
    title , start = GetSessionEndPoint(text)
conn.commit()

print u'ly detail data Succeed'     

def AddPartyPolitic(politic,category,party):
    c.execute('''INSERT into politics(politic,category,party) 
        SELECT %s,%s,%s
        WHERE NOT EXISTS (SELECT politic,category,party FROM politics WHERE politic = %s AND party = %s )''',(politic,category,party,politic,party))  
def AddPolitic(id,politic,category):
    c.execute('''INSERT into politics(id,politic,category) 
        SELECT %s,%s,%s
        WHERE NOT EXISTS (SELECT id,politic,category FROM politics WHERE id = %s AND politic = %s )''',(id,politic,category,id,politic))  
sourcetext = codecs.open(u"08立委政見.txt", "r", "utf-8")
ly = []
for line in sourcetext.readlines():
    line = line.strip()
    if not line:
        continue
    if len(line) < 5:
        rowcount,lyid = LyID(line)
        if rowcount == 1 and lyid:
            ly = lyid[0]
            print line
        else:
            print 'can not find ly' , line
    else:
        if ly:
            if re.search(u'[：]$',line):
                AddPolitic(ly,line,0)
            else:
                AddPolitic(ly,line,1)
conn.commit()
print u'08立委政見Succeed'
sourcetext = codecs.open(u"08政黨政見.txt", "r", "utf-8")
party = []
for line in sourcetext.readlines():
    line = line.strip()
    if not line:
        continue
    if len(line) < 7 and re.search(u'(?<!：)$',line):
        party = line
        print line
    else:
        if party:
            if re.search(u'[：]$',line):
                AddPartyPolitic(line,0,party)
            else:
                AddPartyPolitic(line,1,party)
conn.commit()

print u'08政黨政見Succeed'



