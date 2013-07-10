#! /usr/bin/env python
# -*- coding: utf-8 -*-
import re
import codecs
import psycopg2
from datetime import datetime

def GetSessionROI(text):
    ms , me = re.search(u'立法院第(\d){1,2}屆第[\d]{1,2}會期第[\d]{1,2}次(臨時會第\d次|談話)?會(議)?議事錄',text) , None
    if ms:
        me = re.search(u'立法院第(\d){1,2}屆第[\d]{1,2}會期第[\d]{1,2}次(臨時會第\d次|談話)?會(議)?議事錄',text[1:])     
    return ms , me
def LyID(name):
    c.execute('''SELECT id FROM legislator WHERE name = %s''',[name])
    return c.rowcount,c.fetchone()
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
def AddVote(content,date,session):
    c.execute('''INSERT into vote(content,date,session) 
        SELECT %s,%s,%s
        WHERE NOT EXISTS (SELECT content,date,session FROM vote WHERE content = %s ) RETURNING id''',(content,date,session,content))  
    r = c.fetchone()
    if r:
        return r[0]
def GetVote(text):
    l = text.rstrip().split('\n')
    i = -2
    if re.search(u'[：:]$',l[i].rstrip()):
        return l[-1].lstrip()
    while re.search(u'(?<!附後\S(\d){1,3}\S)[。」]$',l[i].rstrip()): # 句點」作結
        l[i] = l[i].strip()
        i -= 1
    return ('\n'.join(l[i+1:])).lstrip()
def MakeVoteRelation(id,vote_id,decision):
    c.execute('''INSERT into legislator_vote(legislator_id,vote_id,decision)
        SELECT %s,%s,%s
        WHERE NOT EXISTS (SELECT legislator_id,vote_id,decision FROM legislator_vote WHERE legislator_id = %s AND vote_id = %s)''',(legislator_id,vote_id,decision,legislator_id,vote_id))  
def LiterateVoter(text,vote_id,decision):
    firstName = ''
    for name in text.split():      
        #兩個字的立委中文名字中間有空白
        if len(name)<2 and firstName=='':
            firstName = name
            continue
        if len(name)<2 and firstName!='':
            name = firstName + name
            firstName = ''
        #
        rowcount,lyid = LyID(name)
        if rowcount == 1 and lyid:
            MakeVoteRelation(lyid[0],vote_id,decision)
        else:
            break
def IterVote(text,date,session):
    match = None
    mvoter = re.search(u'記名(投票)?表決結果名單',text) 
    if mvoter:
        votertext = text[mvoter.end():]
        for match in re.finditer(u'附後[（(】。]',text):
            mapprove , mreject , mquit = re.search(u'贊成者：[\d]{1,3}人',votertext) , re.search(u'反對者：[\d]{1,3}人',votertext) , re.search(u'棄權者：[\d]{1,3}人',votertext)        
            l = text[:match.end()].split()
            if re.search(u'(附後\S(\d){1,3}\S|草案)',l[-2]) or len(l[-2]) < 10:
                if mquit:
                    votertext = votertext[mquit.end():]
                continue
            vote = '\n'.join(l[-2:])
            vote_id = AddVote(vote,date,session)
            if vote_id:
                if not mapprove:
                    print '==找不到贊成者==\n' , votertext
                else:
                    LiterateVoter(votertext[mapprove.end():],vote_id, 1)
                if not mreject:
                    print '==找不到反對者==\n' , votertext
                else:
                    LiterateVoter(votertext[mreject.end():],vote_id, -1)
                if not mquit:
                    print '==找不到棄權者==\n' , votertext
                else:
                    LiterateVoter(votertext[mquit.end():],vote_id, 0)
                    votertext = votertext[mquit.end():]
        if not match:
            print '有記名表決結果名單無附後'
    else:
        print '無記名表決結果名單'
conn = psycopg2.connect(dbname='dbname', host='ip', user='user' , password='password') 
c = conn.cursor()
sourcetext = codecs.open(u"立院議事錄08_01_03.txt", "r", "utf-8").read()
ms , me = GetSessionROI(sourcetext)
while ms:
    if me:
        singleSessionText = sourcetext[ms.start():me.start()+1]
    else:
        singleSessionText = sourcetext
        session = ms.group()    
        date = GetDate(singleSessionText)
        print session , date
        IterVote(singleSessionText,date,session)
        break
    session = ms.group()  
    date = GetDate(singleSessionText)
    print session , date
    IterVote(singleSessionText,date,session)         
    sourcetext = sourcetext[me.start()+1:]
    ms , me = GetSessionROI(sourcetext)
conn.commit()
print 'Succeed'


