#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../')
import re
import codecs
import psycopg2
import glob
from datetime import datetime
import db_ly
import ly_common


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
        legislator_id = ly_common.GetLegislatorId(name)
        if legislator_id:
            MakeProposalRelation(legislator_id, proposal_id, priproposer)
        else:
            #print name
            break
        priproposer = False

def GetSession(text):
    ms, me, uid = re.search(u'立法院(第(?P<ad>[\d]+)屆第(?P<session>[\d]+)會期(第(?P<times>[\d]+)次臨時會)?(?P<committee>[\W\s]{2,39})(兩|三|四|五|六|七|八)?委員會[\s]*第(?P<temptimes>[\d]+)次(全體委員|聯席)會議)議事錄',text), re.search(u"[\s]+散[\s]{0,4}會",text), None
    if ms:
        if ms.group('temptimes'):
            uid = '%02d-%02dT%02d-%s-%02d' % (int(ms.group('ad')), int(ms.group('session')), int(ms.group('times')), ms.group('committee'), int(ms.group('temptimes')))
        else:
            uid = '%02d-%02d-%s-%02d' % (int(ms.group('ad')), int(ms.group('session')), ms.group('committee'), int(ms.group('times')))
    return ms, me, uid

def GetProposer(text):
    match = None
    for match in re.finditer(u'[\s]*[(（]?(提案人|提案及連署人)：',text):
        pass    
    return match

def GetProposal(text):
    l = text.rstrip().split()
    i = -2
    while re.search(u'(?<!通過|同意|辦理|保留|處理)[？?。」】!！]$',l[i].rstrip()) and re.search(u'^(?!決議[：:])',l[i].lstrip()):   # (?<!通過|同意|辦理|保留):不是這些關鍵字+標點符號作結的為提案內容
        l[i] = l[i].strip()
        i -= 1
    if i == -2:
        return l[-1]
    if re.search(u'[：:]$',l[i].rstrip()):
        if re.search(u'說明[：:]$',l[i]):
            return ('\n'.join(l[i-1:]))
        else:
            return ('\n'.join(l[i:]))       
    else:
        return ('\n'.join(l[i+1:]))

def AddAttendanceRecord(LegislatorID,date,sessionPrd,session,PresentNum,UnpresentNum):
    c.execute('''INSERT into legislator_attendance(legislator_id,date,"sessionPrd",session,category,"presentNum","unpresentNum")
        SELECT %s,%s,%s,%s,1,%s,%s
        WHERE NOT EXISTS (SELECT legislator_id,date,"sessionPrd",session,category,"presentNum","unpresentNum" FROM legislator_attendance WHERE legislator_id = %s AND session = %s)''',(LegislatorID,date,sessionPrd,session,PresentNum,UnpresentNum,LegislatorID,session))  

def GetLegislatorIdList(text):
    id_list, firstName = [], ''
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
        legislator_id = ly_common.GetLegislatorId(name)
        if legislator_id:
            id_list.append(legislator_id)
            ly_common.AddAttendanceRecord(legislator_id, date, sessionPrd, session, 0, 1)
        else:   # return id list if not an legislator name
            return id_list

def GetUnpresent(text, sitting_id):
    match = None
    for match in re.finditer(u'請假委員[：]?',text):
        LiterateLY(text[match.end():], sitting_id)

conn = db_ly.con()
c = conn.cursor()
files = [f for f in glob.glob('./*.txt')]
for f in files:
    text = codecs.open(f, "r", "utf-8").read()
    ms ,me, uid = GetSession(text)
    while ms and me:
        print ms.group(1)
        sitting_dict = {"uid": uid, "name": ms.group(1), "date": ly_common.GetDate(text), "ad": ms.group('ad'), "session": ms.group('session'), "committee": ms.group('committee') }
        ly_common.InsertSitting(c, sitting_dict)
        singleSession = text[ms.start():me.end()]
        filelogid = ly_common.FileLog(c, ms.group(1))
        #if not filelogid:
        #    continue
        proposerS = GetProposer(singleSession)
        GetUnpresent(singleSession, uid)       
        while proposerS:
            proposal = GetProposal(singleSession[:proposerS.start()])
            pid = AddProposal(ms.group('committee'),proposal)
            if pid: #如果該proposal insert成功
                LiterateProposer(singleSession[proposerS.end():],pid)
            proposerS = GetProposer(singleSession[:proposerS.start()])
        text = text[me.end():]
        ms,me = GetSession(text)
conn.commit()
print 'Succeed'
