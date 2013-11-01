#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys # 正常模組
sys.path.append('../')
import re,codecs,psycopg2,glob
import db_ly
from datetime import datetime

def FileLog(session):
    c.execute('''INSERT into legislator_filelog(session,date)
        SELECT %s,%s
        WHERE NOT EXISTS (SELECT session FROM legislator_filelog WHERE session = %s) RETURNING id''',(session,datetime.now(),session))
    #r = c.fetchone()
    #if r:
    #    return r[0]
def LyID(name):
    c.execute('''SELECT id FROM legislator_legislator WHERE name = %s''',[name])
    return c.rowcount,c.fetchone()
def AddProposal(committee,content,sessionPrd,session,date):
    c.execute('''INSERT into proposal_proposal(committee,content,"sessionPrd",session,date) 
        SELECT %s,%s,%s,%s,%s
        WHERE NOT EXISTS (SELECT committee,content,"sessionPrd",session,date FROM proposal_proposal WHERE committee = %s AND content = %s ) RETURNING id''',(committee,content,sessionPrd,session,date,committee,content))  
    r = c.fetchone()
    if r:
        return r[0]
def MakeProposalRelation(legislator_id,proposal_id,priproposer):
    c.execute('''INSERT into proposal_legislator_proposal(legislator_id,proposal_id,priproposer)
        SELECT %s,%s,%s
        WHERE NOT EXISTS (SELECT legislator_id,proposal_id,priproposer FROM proposal_legislator_proposal WHERE legislator_id = %s AND proposal_id = %s)''',(legislator_id,proposal_id,priproposer,legislator_id,proposal_id))  
def LiterateProposer(text,proposal_id):
    firstName,priproposer = '',True
    for name in text.split():      
        if re.search(u'[）)。】」]$',name):   #立委名字後有標點符號
            name = name[:-1]
        #兩個字的立委中文名字中間有空白
        if len(name)<2 and firstName=='':
            firstName = name
            continue
        if len(name)<2 and firstName!='':
            name = firstName + name
            firstName = ''      
        if len(name)>4: #立委名字相連或名字後加英文
            name = name[:3]
        rowcount,lyid = LyID(name)
        if rowcount == 1 and lyid:
            MakeProposalRelation(lyid[0],proposal_id,priproposer)
        else:
            #print name
            break
        priproposer = False
def GetSession(text):
    return re.search(u'立法院第(?P<sessionPrd>\d){1,2}屆第[\d]{1,2}會期(第[\d]{1,2}次臨時會)?(?P<committee>[\W\s]{2,39})(兩|三|四|五|六|七|八)?委員會[\s]*第[\d]{1,2}次(全體委員|聯席)會議議事錄',text) , re.search(u"[\s]+散[\s]{0,4}會",text)
def GetDate(text):
    matchTerm = re.search(u'(\d)+[\s]?年(\d)+[\s]?月(\d)+',text)
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
def GetProposer(text):
    match = None
    for match in re.finditer(u'[\s]*[(（]?(提案人|提案及連署人)：',text):
        pass    
    return match
def GetProposal(text):
    l = text.rstrip().split('\n')
    i = -2
    while re.search(u'(?<!通過|同意|辦理|保留|處理)[？?。」】!！]$',l[i].rstrip()):   # (?<!通過|同意|辦理|保留):不是這些關鍵字+標點符號作結的為提案內容
        l[i] = l[i].strip()
        i -= 1
    if i == -2:
        return l[-1].lstrip()
    if re.search(u'[：:]$',l[i].rstrip()):
        if re.search(u'說明[：:]$',l[i].rstrip()):
            return ('\n'.join(l[i-1:])).lstrip()
        else:
            return ('\n'.join(l[i:])).lstrip()
    else:
        return ('\n'.join(l[i+1:])).lstrip()
def AddAttendanceRecord(LegislatorID,date,sessionPrd,session,PresentNum,UnpresentNum):
    c.execute('''INSERT into legislator_attendance(legislator_id,date,"sessionPrd",session,category,"presentNum","unpresentNum")
        SELECT %s,%s,%s,%s,1,%s,%s
        WHERE NOT EXISTS (SELECT legislator_id,date,"sessionPrd",session,category,"presentNum","unpresentNum" FROM legislator_attendance WHERE legislator_id = %s AND session = %s)''',(LegislatorID,date,sessionPrd,session,PresentNum,UnpresentNum,LegislatorID,session))  

def LiterateLY(text,date,sessionPrd,session):
    firstName = ''
    for name in text.split():      
        if re.search(u'[）)。】」]$',name):   #立委名字後有標點符號
            name = name[:-1]
        #兩個字的立委中文名字中間有空白
        if len(name)<2 and firstName=='':
            firstName = name
            continue
        if len(name)<2 and firstName!='':
            name = firstName + name
            firstName = ''
        if len(name)>4: #立委名字相連
            name = name[:3]
        rowcount,lyid = LyID(name)
        if rowcount == 1 and lyid:
            #print name
            AddAttendanceRecord(lyid[0],date,sessionPrd,session,0,1)
        else:
            break
def GetUnpresent(text,date,sessionPrd,session):
    match = None
    for match in re.finditer(u'[\s]*請假委員[：]?',text):
        LiterateLY(text[match.end():],date,sessionPrd,session)

conn = db_ly.con()
c = conn.cursor()
files = [f for f in glob.glob('./*.txt')]
for f in files:
    text = codecs.open(f, "r", "utf-8").read()
    ms,me = GetSession(text)
    while ms and me:
        singleSession = text[ms.start():me.end()]
        sessionName = ms.group().replace('\n','')
        filelogid = FileLog(sessionName)
        #if not filelogid:
        #    continue
        sessionDate = GetDate(singleSession)
        proposerS = GetProposer(singleSession)
        GetUnpresent(singleSession,sessionDate,ms.group('sessionPrd'),sessionName)       
        while proposerS:
            print sessionName
            proposal = GetProposal(singleSession[:proposerS.start()])
            pid = AddProposal(ms.group('committee'),proposal,ms.group('sessionPrd'),sessionName,sessionDate)
            if pid: #如果該proposal insert成功
                LiterateProposer(singleSession[proposerS.end():],pid)
            proposerS = GetProposer(singleSession[:proposerS.start()])
        text = text[me.end():]
        ms,me = GetSession(text)
conn.commit()
print 'Succeed'
