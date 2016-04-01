#! /usr/bin/env python
# -*- coding: utf-8 -*-
import sys
sys.path.append('../')
import os
import re
import json
import codecs
import psycopg2
from datetime import datetime

import vote_common
from common import ly_common
from common import db_settings


def GetVoteContent(vote_seq, text):
    l = text.split()
    if re.search(u'附後[（(】。]', l[-2]) or re.search(u'^(其他事項|討論事項)$', l[-2]):
        return l[-1]
    if re.search(u'[：:]$', l[-2]) or re.search(u'(公決|照案|議案)[\S]{0,3}$', l[-2]) or re.search(u'^(決議|決定)[：:]', l[-1]):
        return '\n'.join(l[-2:])
    if re.search(u'[：:]$',l[-3]):
        return '\n'.join(l[-3:])
    i = -3
    # 法條修正提案列表類
    if ly_common.GetLegislatorId(c, l[-2]) or ly_common.GetLegislatorId(c, l[-3]) or re.search(u'(案|審查)[\S]{0,3}$', l[-2]):
        while not re.search(u'(通過|附表|如下|討論)[\S]{1,2}$', l[i]):
            i -= 1
        return '\n'.join(l[i:])
    # 剩下的先向上找上一個附後，找兩附後之間以冒號作結，如找不到
    if vote_seq != '001':
        while not re.search(u'附後[（(】。]', l[i]):
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
    print text
    print 'WTF!!!'
    raw_input()

def LiterateVoter(sitting_dict, text, vote_id, decision):
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
            vote_common.upsert_vote_legislator_vote(c, legislator_id, vote_id, decision)
        else:
            break

def IterEachDecision(votertext, sitting_dict, vote_id):
    mapprove, mreject, mquit = re.search(u'[\s、]贊成[\S]*?者[:：][\d]+人', votertext), re.search(u'[\s、]反對[\S]*?者[:：][\d]+人', votertext), re.search(u'[\s、]棄權者[:：][\d]+人', votertext)
    if not mapprove:
        print u'==找不到贊成者==\n', votertext
        raw_input()
    else:
        LiterateVoter(sitting_dict, votertext[mapprove.end():], vote_id, 1)
    if not mreject:
        print u'==找不到反對者==\n', votertext
        raw_input()
    else:
        LiterateVoter(sitting_dict, votertext[mreject.end():], vote_id, -1)
    if not mquit:
        print u'==找不到棄權者==\n', votertext
    else:
        LiterateVoter(sitting_dict, votertext[mquit.end():], vote_id, 0)
    return mapprove, mreject, mquit

def IterVote(text, sitting_dict):
    sitting_id = sitting_dict["uid"]
    print sitting_id
    match, vote_id, vote_seq = None, None, 0
    # For veto or no-confidence voting
    mvoter = re.search(u'記名投票表決結果[:：]', text)
    if mvoter:
        print u'有特殊表決!!\n'
        votertext = text[mvoter.end():]
        vote_seq = '%03d' % (vote_seq+1)
        vote_id = '%s-%s' % (sitting_id, vote_seq)
        content = GetVoteContent(vote_seq, text[:mvoter.start()])
        if content:
            vote_common.upsert_vote(c, vote_id, sitting_id, vote_seq, content)
            mapprove, mreject, mquit = IterEachDecision(votertext, sitting_dict, vote_id)
    # For normal voting
    mvoter = re.search(u'記名表決結果名單[:：]', text)
    if mvoter:
        votertext = text[mvoter.end():]
        for match in re.finditer(u'附後[（(】。]', text):
            vote_seq = '%03d' % (int(vote_seq)+1)
            vote_id = '%s-%s' % (sitting_id, vote_seq)
            content = GetVoteContent(vote_seq, text[:match.start()+2])
            if content:
                vote_common.upsert_vote(c, vote_id, sitting_id, vote_seq, content)
                mapprove, mreject, mquit = IterEachDecision(votertext, sitting_dict, vote_id)
            votertext = votertext[(mquit or mreject or mapprove).end():]
        if not match:
            print u'有記名表決結果名單無附後'
    else:
        print u'無記名表決結果名單'

conn = db_settings.con()
c = conn.cursor()
ad = 7
dicts = json.load(open('vote/minutes.json'))
for meeting in dicts:
    print '[%s]' % meeting['name']
    #--> meeting info already there but meeting_minutes haven't publish
    if not os.path.exists('vote/meeting_minutes/%s.txt' % meeting['name']):
        print 'File not exist, please check!!'
        raw_input()
        continue
    #<--
    sourcetext = codecs.open(u'vote/meeting_minutes/%s.txt' % meeting['name'], 'r', 'utf-8').read()
    ms, uid = ly_common.SittingDict(meeting['name'])
    date = ly_common.GetDate(sourcetext)
    if int(ms.group('ad')) != ad or uid in sitting_ids:
        print 'Skip'
        continue
    else:
        if not date:
            print 'Can not find meeting date from minutes, please check file!!'
            raw_input()
            continue
    sitting_dict = {"uid": uid, "name": meeting['name'], "ad": ms.group('ad'), "date": date, "session": ms.group('session'), "links": meeting['links']}
    ly_common.InsertSitting(c, sitting_dict)
    ly_common.FileLog(c, meeting['name'])
    ly_common.Attendance(c, sitting_dict, sourcetext, u'出席委員[:：]?', 'YS', 'present')
    ly_common.Attendance(c, sitting_dict, sourcetext, u'請假委員[:：]?', 'YS', 'absent')
    IterVote(sourcetext, sitting_dict)

vote_common.conscience_vote(c, ad)
vote_common.not_voting_and_results(c)
vote_common.vote_param(c)
vote_common.attendance_param(c)
conn.commit()
