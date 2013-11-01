#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys # 正常模組
sys.path.append('../')
import re,codecs,psycopg2
import db_ly,ly_common
from datetime import datetime

def LyID(name):
    c.execute('''SELECT id FROM legislator_legislator WHERE name = %s''',[name])
    return c.rowcount,c.fetchone()
def AddVote(content,date,session):
    c.execute('''INSERT into vote_vote(content,date,session,hits) 
        SELECT %s,%s,%s,%s
        WHERE NOT EXISTS (SELECT content,date,session,hits FROM vote_vote WHERE content = %s ) RETURNING id''',(content,date,session,0,content))  
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
def MakeVoteRelation(legislator_id,vote_id,decision):
    c.execute('''INSERT into vote_legislator_vote(legislator_id,vote_id,decision)
        SELECT %s,%s,%s
        WHERE NOT EXISTS (SELECT legislator_id,vote_id,decision FROM vote_legislator_vote WHERE legislator_id = %s AND vote_id = %s)''',(legislator_id,vote_id,decision,legislator_id,vote_id))  
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
            #print name
        #
        rowcount,lyid = LyID(name)
        if rowcount == 1 and lyid:
            MakeVoteRelation(lyid[0],vote_id,decision)
        else:
            break
def IterVote(text,date,session):
    match = None
    mvoter = re.search(u'記名(投票)?表決結果名單：',text) 
    if mvoter:
        votertext = text[mvoter.end():]
        for match in re.finditer(u'附後[（(】。]',text):
            mapprove , mreject , mquit = re.search(u'贊成者：[\d]{1,3}人',votertext) , re.search(u'反對者：[\d]{1,3}人',votertext) , re.search(u'棄權者：[\d]{1,3}人',votertext)        
            l = text[:match.end()].split()
            if re.search(u'(附後\S(\d){1,3}\S)',l[-2]) or re.search(u'草案[」](。)?$',l[-2]) or len(l[-2]) < 10:
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
conn = db_ly.con()
c = conn.cursor()
sourcetext = codecs.open(u"立院議事錄08_01_03.txt", "r", "utf-8").read()
ms , me = ly_common.GetSessionROI(sourcetext)
while ms:
    if me:
        singleSessionText = sourcetext[ms.start():me.start()+1]
    else:
        singleSessionText = sourcetext
        session = ms.group()    
        date = ly_common.GetDate(singleSessionText)
        print session , date
        IterVote(singleSessionText,date,session)
        break
    session = ms.group()    
    date = ly_common.GetDate(singleSessionText)
    print session , date
    IterVote(singleSessionText,date,session)         
    sourcetext = sourcetext[me.start()+1:]
    ms , me = ly_common.GetSessionROI(sourcetext)
conn.commit()
def party_Decision_List(party):
    c.execute('''select vote_id,avg(decision) from vote_legislator_vote
    where legislator_id in (select id from legislator_legislator where party=%s)
    group by vote_id''',(party,))
    return c.fetchall()
def personal_Decision_List(party,Vote_id):
    c.execute('''select legislator_id,decision from vote_legislator_vote
    where legislator_id in (select id from legislator_legislator where party=%s) and vote_id = %s''',(party,Vote_id))
    return c.fetchall()
def party_List():
    c.execute('''select distinct(party) from legislator_legislator''')
    return c.fetchall()
def conflict_vote(vote_id):
    c.execute('''update vote_vote set conflict=True where id=%s''',(vote_id,))
def conflict_legislator_vote(legislator_id,vote_id):
    c.execute('''update vote_legislator_vote set conflict=True where legislator_id=%s and vote_id=%s''',(legislator_id,vote_id))
for party in party_List():
    if party != u'無':
        for v in party_Decision_List(party):
            if int(v[1]) != v[1]:
                conflict_vote(v[0])
                for p in personal_Decision_List(party,v[0]):
                    if p[1]*v[1] <= 0:
                        conflict_legislator_vote(p[0],v[0])      
conn.commit()        
print 'Succeed'


