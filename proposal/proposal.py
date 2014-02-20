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


def InsertUpdateProposal(uid, sitting_id, proposal_seq, content):
    c.execute('''
        UPDATE proposal_proposal
        SET content = %s
        WHERE uid = %s
    ''', (content, uid))
    c.execute('''
        INSERT into proposal_proposal(uid, sitting_id, proposal_seq, content, hits, likes, dislikes)
        SELECT %s, %s, %s, %s, 0, 0, 0
        WHERE NOT EXISTS (SELECT 1 FROM proposal_proposal WHERE uid = %s )
    ''', (uid, sitting_id, proposal_seq, content, uid))

def InsertUpdateLegislatorProposal(legislator_id, proposal_id, priproposer):
    c.execute('''
        UPDATE proposal_legislator_proposal
        SET priproposer = %s
        WHERE legislator_id = %s AND proposal_id = %s
    ''', (priproposer, legislator_id, proposal_id))
    c.execute('''
        INSERT into proposal_legislator_proposal(legislator_id, proposal_id, priproposer)
        SELECT %s, %s, %s
        WHERE NOT EXISTS (SELECT 1 FROM proposal_legislator_proposal WHERE legislator_id = %s AND proposal_id = %s)
    ''', (legislator_id, proposal_id, priproposer, legislator_id, proposal_id))

def GetSession(text):
    ms, me, uid = re.search(u'立法院(第(?P<ad>[\d]+)屆第(?P<session>[\d]+)會期(第(?P<temptimes>[\d]+)次臨時會)?(?P<committee>[\W\s]{2,39})(兩|三|四|五|六|七|八)?委員會[\s]*第(?P<times>[\d]+)次(全體委員|聯席)會議)議事錄', text), re.search(u'散[\s]*會', text), None
    if ms:
        ms.group('committee').split(u'、')
        if ms.group('temptimes'):
            uid = '%02d-%02dT%02d-%s-%02d' % (int(ms.group('ad')), int(ms.group('session')), int(ms.group('temptimes')), ms.group('committee'), int(ms.group('times')))
        else:
            uid = '%02d-%02d-%s-%02d' % (int(ms.group('ad')), int(ms.group('session')), ms.group('committee'), int(ms.group('times')))
    return ms, me, uid

def GetProposer(text):
    match, last_seq = None, 0
    for match in re.finditer(u'[(（]?(提案人|提案及連署人)：', text):
        last_seq += 1
    return match, last_seq

def GetContent(text):
    l = text.rstrip().split('\n')
    i = -2
    while re.search(u'(?<!通過|同意|辦理|保留|處理)[？?。」】!！]$', l[i].rstrip()) and re.search(u'^(?!決議[：:])', l[i].lstrip()):   # (?<!通過|同意|辦理|保留):不是這些關鍵字+標點符號作結的為提案內容
        l[i] = l[i].strip()
        i -= 1
    if i == -2:
        return l[-1].lstrip()
    if re.search(u'[：:]$', l[i].rstrip()):
        if re.search(u'說明[：:]$', l[i]):
            return ('\n'.join(l[i-1:])).lstrip()
        else:
            return ('\n'.join(l[i:])).lstrip()
    else:
        return ('\n'.join(l[i+1:])).lstrip()

conn = db_ly.con()
c = conn.cursor()
files = [f for f in glob.glob('./*.txt')]
for f in files:
    text = codecs.open(f, "r", "utf-8").read()
    ms ,me, uid = GetSession(text)
    while ms and me:
        sitting_dict = {"uid": uid, "name": re.sub(u'[\s]', '', ms.group(1)), "date": ly_common.GetDate(text), "ad": ms.group('ad'), "session": ms.group('session'), "committee": ms.group('committee') }
        print sitting_dict.get("name")
        ly_common.InsertSitting(c, sitting_dict)
        singleSession = text[ms.start():me.end()]
        filelogid = ly_common.FileLog(c, sitting_dict.get("name"))
        ly_common.Attendance(c, sitting_dict, singleSession, u'出席委員[:：]?', 'committee', 'present')
        ly_common.Attendance(c, sitting_dict, singleSession, u'列席委員[:：]?', 'committee', 'attend')
        ly_common.Attendance(c, sitting_dict, singleSession, u'請假委員[:：]?', 'committee', 'absent')
        proposerS, last_seq = GetProposer(singleSession)
        proposal_seq = 0
        for proposal_seq in reversed(range(1, last_seq+1)):
            print proposal_seq
            proposal_id = '%s-%03d' % (uid, proposal_seq)
            content = GetContent(singleSession[:proposerS.start()])
            InsertUpdateProposal(proposal_id, uid, proposal_seq, content)
            priproposer = True
            for legislator_id in ly_common.GetLegislatorIdList(c, singleSession[proposerS.end():]):
                legislator_id = ly_common.GetLegislatorDetailId(c, legislator_id, sitting_dict["ad"])
                InsertUpdateLegislatorProposal(legislator_id, proposal_id, priproposer)
                priproposer = False
            proposerS, foo = GetProposer(singleSession[:proposerS.start()])
        text = text[me.end():]
        ms ,me, uid = GetSession(text)
conn.commit()
print 'Succeed'
