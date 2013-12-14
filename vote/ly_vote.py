#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys 
sys.path.append('../')
import re
import codecs
import psycopg2
from datetime import datetime
import db_ly
import ly_common


def AddVote(sitting_id, vote_seq, content):
    c.execute('''INSERT into vote_vote(sitting_id, vote_seq, content, hits, likes, dislikes) 
        SELECT %s, %s, %s, 0, 0, 0
        WHERE NOT EXISTS (SELECT 1 FROM vote_vote WHERE sitting_id = %s AND vote_seq = %s ) RETURNING id''',(sitting_id, vote_seq, content, sitting_id, vote_seq))  
    r = c.fetchone()
    if r:
        return r[0]
def GetVote(text):
    l = text.split()
    i = -2
    if re.search(u'(附後\S[\d]+\S)', l[-2]):
        return l[-1]
    if re.search(u'[：:]$',l[-2]):
        return '\n'.join(l[-2:])

def MakeVoteRelation(legislator_id,vote_id,decision):
    c.execute('''INSERT into vote_legislator_vote(legislator_id,vote_id,decision)
        SELECT %s,%s,%s
        WHERE NOT EXISTS (SELECT legislator_id,vote_id,decision FROM vote_legislator_vote WHERE legislator_id = %s AND vote_id = %s)''',(legislator_id,vote_id,decision,legislator_id,vote_id))  
def LiterateVoter(c, text,vote_id,decision):
    firstName = ''
    for name in text.split():      
        #--> 兩個字的立委中文名字中間有空白
        if len(name)<2 and firstName=='':
            firstName = name
            continue
        if len(name)<2 and firstName!='':
            name = firstName + name
            firstName = ''
        #<--
        legislator_id = ly_common.GetLegislatorId(c, name)
        if legislator_id:
            MakeVoteRelation(legislator_id,vote_id,decision)
        else:
            break
def IterVote(c, text, uid):
    print uid
    match, vote_id = None, None
    mvoter = re.search(u'記名(投票)?表決結果名單[:：]', text) 
    if mvoter:
        votertext = text[mvoter.end():]
        for match in re.finditer(u'附後[（(】。](?P<vote_seq>[\d]+)?', text):
            if match.group('vote_seq'):
                vote_seq = '%02d' % int(match.group('vote_seq'))
            else:
                vote_seq = '01'
            mapprove , mreject , mquit = re.search(u'贊成者[:：][\d]+人', votertext) , re.search(u'反對者[:：][\d]+人', votertext) , re.search(u'棄權者[:：][\d]+人', votertext)        
            #l = text[:match.end()-1].split()
            #if re.search(u'(附後\S(\d){1,3}\S)', l[-2]) or re.search(u'草案[」](。)?$', l[-2]) or len(l[-2]) < 10:
            #   if mquit:
            #       votertext = votertext[mquit.end():]
            #   continue
            content = GetVote(text[:match.end()-1])
            if content:
                vote_id = AddVote(uid, vote_seq, content)
            if vote_id:
                if not mapprove:
                    print u'==找不到贊成者==\n' ,votertext
                else:
                    LiterateVoter(c, votertext[mapprove.end():], vote_id, 1)
                if not mreject:
                    print u'==找不到反對者==\n' ,votertext
                else:
                    LiterateVoter(c, votertext[mreject.end():], vote_id, -1)
                if not mquit:
                    print u'==找不到棄權者==\n' ,votertext
                else:
                    LiterateVoter(c, votertext[mquit.end():], vote_id, 0)
            votertext = votertext[mquit.end():]
        if not match:
            print u'有記名表決結果名單無附後'
    else:
        print u'無記名表決結果名單'
conn = db_ly.con()
c = conn.cursor()
sourcetext = codecs.open(u"立院議事錄08.txt", "r", "utf-8").read()
ms ,me, uid = ly_common.GetSessionROI(sourcetext)
while ms:
    if me:
        singleSessionText = sourcetext[ms.start():me.start()+1]
    else: # last session
        singleSessionText = sourcetext
        IterVote(c, singleSessionText, uid)
        break
    IterVote(c, singleSessionText, uid)         
    sourcetext = sourcetext[me.start()+1:]
    ms ,me, uid = ly_common.GetSessionROI(sourcetext)
conn.commit()
# --> conscience vote
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
# <-- conscience vote

# --> not voting
def vote_list():
    c.execute('''select id, date from vote_vote''')
    return c.fetchall()
def not_voting_legislator_list(vote_id,vote_date):
    c.execute('''select id
                from legislator_legislator
                where term_start <= %s and
                    term_end > %s and
                    id not in (select legislator_id
                    from vote_legislator_vote 
                    where vote_id = %s)''',(vote_date,vote_date,vote_id))
    return c.fetchall()
def insert_not_voting_record(legislator_id,vote_id):
    c.execute('''INSERT into vote_legislator_vote(legislator_id,vote_id)
        SELECT %s,%s
        WHERE NOT EXISTS (SELECT legislator_id,vote_id FROM vote_legislator_vote WHERE legislator_id = %s AND vote_id = %s)''',(legislator_id,vote_id,legislator_id,vote_id))   
for vote_id, vote_date in vote_list():
    for legislator_id in not_voting_legislator_list(vote_id,vote_date):
        insert_not_voting_record(legislator_id, vote_id)
conn.commit()
# <-- not voting end

# --> vote result


# <-- vote result end
print 'Succeed'


