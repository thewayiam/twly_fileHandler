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


def GetSessionROI(text):
    ms ,me, uid = re.search(u'''
        立法院
        (?P<name>
            第(?P<ad>[\d]+)屆
            第(?P<session>[\d]+)會期
            第(?P<times>[\d]+)次
            (臨時會第(?P<temptimes>[\d]+)次)?
            會議
        )
        議事錄
    ''', text, re.X) , None, None
    if ms:
        if ms.group('temptimes'):
            uid = '%02d-%02dT%02d-YS-%02d' % (int(ms.group('ad')), int(ms.group('session')), int(ms.group('times')), int(ms.group('temptimes')))
        else:
            uid = '%02d-%02d-YS-%02d' % (int(ms.group('ad')), int(ms.group('session')), int(ms.group('times')))
        me = re.search(u'立法院第(\d){1,2}屆第[\d]{1,2}會期第[\d]{1,2}次(臨時會第\d次)?會議議事錄', text[1:])
    return ms ,me, uid

def InsertVote(uid, sitting_id, vote_seq, content):
    match = re.search(u'建請(?:院會|本院).*(?:請公決案)', content)
    summary = ''
    if match:
        summary = match.group()
    c.execute('''
        UPDATE vote_vote
        SET summary = %s
        WHERE uid = %s
    ''', (summary, uid))
    #c.execute('''
    #    UPDATE vote_vote
    #    SET content = %s, conflict = null
    #    WHERE uid = %s
    #''', (content, uid))
    c.execute('''
        INSERT into vote_vote(uid, sitting_id, vote_seq, content, summary, hits, likes, dislikes)
        SELECT %s, %s, %s, %s, %s, 0, 0, 0
        WHERE NOT EXISTS (SELECT 1 FROM vote_vote WHERE uid = %s)
    ''', (uid, sitting_id, vote_seq, content, summary, uid))

def GetVoteContent(c, vote_seq, text):
    l = text.split()
    if re.search(u'附後\S[\d]+\S', l[-2]) or re.search(u'^(其他事項|討論事項)$', l[-2]):
        return l[-1]
    if re.search(u'[：:]$', l[-2]) or re.search(u'(公決|照案|議案)[\S]{0,3}$', l[-2]) or re.search(u'^(決議|決定)[：:]', l[-1]):
        return '\n'.join(l[-2:])
    if re.search(u'[：:]$',l[-3]):
        return '\n'.join(l[-3:])
    i = -3
    # 法條修正提案列表類
    if ly_common.GetLegislatorId(c, l[-2]) or ly_common.GetLegislatorId(c, l[-3]) or re.search(u'(案|審查)[\S]{0,3}$', l[-2]):
        while not re.search(u'(通過|附表|如下)[\S]{1,2}$', l[i]):
            i -= 1
        return '\n'.join(l[i:])
    # 剩下的先向上找上一個附後，找兩附後之間以冒號作結，如找不到
    if vote_seq != '001':
        while not re.search(u'附後\S[\d]+\S', l[i]):
            i -= 1
        for line in reversed(range(i-1,-3)):
            if re.search(u'[：:]$', l[line]):
                return '\n'.join(l[line:])
        return '\n'.join(l[i+1:])
    # 最後方法
    if re.search(u'^[\S]{1,5}在場委員', l[-1]):
        return '\n'.join(l[-2:])
    else:
        return l[-1]
    print l[-1]

def MakeVoteRelation(legislator_id, vote_id, decision):
    c.execute('''
        UPDATE vote_legislator_vote
        SET decision = %s, conflict = null
        WHERE legislator_id = %s AND vote_id = %s
    ''', (decision, legislator_id, vote_id))
    c.execute('''
        INSERT into vote_legislator_vote(legislator_id, vote_id, decision)
        SELECT %s, %s, %s
        WHERE NOT EXISTS (SELECT 1 FROM vote_legislator_vote WHERE legislator_id = %s AND vote_id = %s)
    ''',(legislator_id, vote_id, decision, legislator_id, vote_id))

def LiterateVoter(c, sitting_dict, text, vote_id, decision):
    firstName = ''
    for name in text.split():
        #--> 兩個字的立委中文名字中間有空白
        if len(name) < 2 and firstName == '':
            firstName = name
            continue
        if len(name) < 2 and firstName != '':
            name = firstName + name
            firstName = ''
        #<--
        legislator_id = ly_common.GetLegislatorId(c, name)
        if legislator_id:
            legislator_id = ly_common.GetLegislatorDetailId(c, legislator_id, sitting_dict["ad"])
            MakeVoteRelation(legislator_id, vote_id, decision)
        else:
            print 'break at: %s' % name
            break

def IterEachDecision(c, votertext, sitting_dict, vote_id):
    mapprove, mreject, mquit = re.search(u'\s贊成[\S]*?者[:：][\d]+人', votertext), re.search(u'\s反對[\S]*?者[:：][\d]+人', votertext), re.search(u'棄權者[:：][\d]+人', votertext)
    if not mapprove:
        print u'==找不到贊成者==\n', votertext
    else:
        LiterateVoter(c, sitting_dict, votertext[mapprove.end():], vote_id, 1)
    if not mreject:
        print u'==找不到反對者==\n', votertext
    else:
        LiterateVoter(c, sitting_dict, votertext[mreject.end():], vote_id, -1)
    if not mquit:
        print u'==找不到棄權者==\n', votertext
    else:
        LiterateVoter(c, sitting_dict, votertext[mquit.end():], vote_id, 0)
    return mapprove, mreject, mquit

def IterVote(c, text, sitting_dict):
    sitting_id = sitting_dict["uid"]
    print sitting_id
    match, vote_id, vote_seq = None, None, '000'
    # For normal voting
    mvoter = re.search(u'記名表決結果名單[:：]', text)
    if mvoter:
        votertext = text[mvoter.end():]
        for match in re.finditer(u'附後[（(】。](?P<vote_seq>[\d]+)?', text):
            if match.group('vote_seq'):
                vote_seq = '%03d' % int(match.group('vote_seq'))
            else:
                vote_seq = '001'
            vote_id = '%s-%s' % (sitting_id, vote_seq)
            content = GetVoteContent(c, vote_seq, text[:match.start()+2])
            if content:
                InsertVote(vote_id, sitting_id, vote_seq, content)
            if vote_id:
                mapprove, mreject, mquit = IterEachDecision(c, votertext, sitting_dict, vote_id)
            votertext = votertext[(mquit or mreject or mapprove).end():]
        if not match:
            print u'有記名表決結果名單無附後'
    else:
        print u'無記名表決結果名單'
    # For veto or no-confidence voting
    mvoter = re.search(u'記名投票表決結果[:：]', text)
    if mvoter:
        print u'有特殊表決!!\n'
        votertext = text[mvoter.end():]
        vote_seq = '%03d' % (int(vote_seq)+1)
        vote_id = '%s-%s' % (sitting_id, vote_seq)
        content = GetVoteContent(c, vote_seq, text[:mvoter.start()])
        if content:
            InsertVote(vote_id, sitting_id, vote_seq, content)
        if vote_id:
            mapprove, mreject, mquit = IterEachDecision(c, votertext, sitting_dict, vote_id)

conn = db_ly.con()
c = conn.cursor()
ad = 8
sourcetext = codecs.open(u"立院議事錄08.txt", "r", "utf-8").read()
ms ,me, uid = GetSessionROI(sourcetext)
while ms:
    print '\n' + ms.group('name')
    sitting_dict = {"uid":uid, "name": ms.group('name'), "ad": ms.group('ad'), "date": ly_common.GetDate(sourcetext), "session": ms.group('session') }
    ly_common.InsertSitting(c, sitting_dict)
    ly_common.FileLog(c, ms.group('name'))
    ly_common.Attendance(c, sitting_dict, sourcetext, u'出席委員[:：]?', 'YS', 'present')
    ly_common.Attendance(c, sitting_dict, sourcetext, u'請假委員[:：]?', 'YS', 'absent')
    if me:
        singleSessionText = sourcetext[ms.start():me.start()+1]
    else: # last session
        singleSessionText = sourcetext
        IterVote(c, singleSessionText, sitting_dict)
        break
    IterVote(c, singleSessionText, sitting_dict)
    sourcetext = sourcetext[me.start()+1:]
    ms ,me, uid = GetSessionROI(sourcetext)
conn.commit()

# --> conscience vote
print u'Conscience vote processing...'
def party_Decision_List(party, ad):
    c.execute('''
        select vote_id, avg(decision)
        from vote_legislator_vote
        where decision is not null and legislator_id in (select id from legislator_legislatordetail where party = %s and ad = %s)
        group by vote_id
    ''', (party, ad))
    return c.fetchall()

def personal_Decision_List(party, vote_id, ad):
    c.execute('''
        select legislator_id, decision
        from vote_legislator_vote
        where decision is not null and vote_id = %s and legislator_id in (select id from legislator_legislatordetail where party = %s and ad = %s)
    ''', (vote_id, party, ad))
    return c.fetchall()

def party_List(ad):
    c.execute('''
        select distinct(party)
        from legislator_legislatordetail
        where ad = %s
    ''', (ad,))
    return c.fetchall()

def conflict_vote(conflict, vote_id):
    c.execute('''
        update vote_vote
        set conflict = %s
        where uid = %s
    ''', (conflict, vote_id))

def conflict_legislator_vote(conflict, legislator_id, vote_id):
    c.execute('''
        update vote_legislator_vote
        set conflict = %s
        where legislator_id = %s and vote_id = %s
    ''', (conflict, legislator_id, vote_id))

for party in party_List(ad):
    if party != u'無黨籍':
        for vote_id, avg_decision in party_Decision_List(party, ad):
            # 黨的decision平均值如不為整數，表示該表決有人脫黨投票
            if int(avg_decision) != avg_decision:
                conflict_vote(True, vote_id)
                # 同黨各立委的decision與黨的decision平均值相乘如小於(相反票)等於(棄權票)零，表示脫黨投票
                for legislator_id, personal_decision in personal_Decision_List(party, vote_id, ad):
                    if personal_decision*avg_decision <= 0:
                        conflict_legislator_vote(True, legislator_id, vote_id)
conn.commit()
print 'done!'
# <-- conscience vote

# --> not voting & vote results
print u'Not voting & vote results processing...'
def vote_list():
    c.execute('''
        select vote.uid, sitting.ad, sitting.date
        from vote_vote vote, sittings_sittings sitting
        where vote.sitting_id = sitting.uid
    ''')
    return c.fetchall()

def not_voting_legislator_list(vote_id, vote_ad, vote_date):
    c.execute('''
        select id
        from legislator_legislatordetail
        where ad = %s and term_start <= %s and cast(term_end::json->>'date' as date) > %s and id not in (select legislator_id from vote_legislator_vote where vote_id = %s)
    ''', (vote_ad, vote_date, vote_date, vote_id))
    return c.fetchall()

def insert_not_voting_record(legislator_id, vote_id):
    c.execute('''
        INSERT INTO vote_legislator_vote(legislator_id, vote_id)
        SELECT %s, %s
        WHERE NOT EXISTS (SELECT legislator_id, vote_id FROM vote_legislator_vote WHERE legislator_id = %s AND vote_id = %s)
    ''', (legislator_id, vote_id, legislator_id, vote_id))

def get_vote_results(vote_id):
    c.execute('''
        select
            count(*) total,
            sum(case when decision isnull then 1 else 0 end) not_voting,
            sum(case when decision = 1 then 1 else 0 end) agree,
            sum(case when decision = 0 then 1 else 0 end) abstain,
            sum(case when decision = -1 then 1 else 0 end) disagree
        from vote_legislator_vote
        where vote_id = %s
    ''', (vote_id,))
    return [desc[0] for desc in c.description], c.fetchone() # return column name and value

def update_vote_results(uid, results):
    if results['agree'] > results['disagree']:
        result = 'Passed'
    else:
        result = 'Not Passed'
    c.execute('''
        UPDATE vote_vote
        SET result = %s, results = %s
        WHERE uid = %s
    ''', (result, results, uid))

for vote_id, vote_ad, vote_date in vote_list():
    for legislator_id in not_voting_legislator_list(vote_id, vote_ad, vote_date):
        insert_not_voting_record(legislator_id, vote_id)
    key, value = get_vote_results(vote_id)
    update_vote_results(vote_id, dict(zip(key, value)))

conn.commit()
print 'done!'
# <-- not voting & vote results end

print 'Succeed'
