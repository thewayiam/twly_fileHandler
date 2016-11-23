#! /usr/bin/env python
# -*- coding: utf-8 -*-
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
    # 傳入的text為"附後"該行以上的所有該次會議內容
    lines = [line.strip() for line in text.split('\n') if line]
    # 此附後的前一行: re.compile(u'附後\S[\d]+\S') or re.compile(u'(其他事項|討論事項)[：:]?$')
    if re.search(u'附後\S[\d]+\S', lines[-2]) or re.search(u'(其他事項|討論事項)[：:]?$', lines[-2]):
        return lines[-1]
    # 此附後的前一行: re.compile(u'[：:]$') or re.compile(u'(公決|照案|議案)[\S]{0,3}$')
    if re.search(u'[：:]$', lines[-2]) or re.search(u'(公決|照案|議案)\S{0,3}$', lines[-2]):
        # 排除增列提議的順序案
        if not re.search(u'^\d+', lines[-3]):
            return '\n'.join(lines[-2:])
    # 此附後所在行的開頭: re.compile(u'^(決議|決定)[：:]')
    if re.search(u'^(決議|決定)[：:]', lines[-1]):
        return '\n'.join(lines[-2:])
    if re.search(u'[：:]$',lines[-3]):
        return '\n'.join(lines[-3:])
    base_line = -3
    # 法條修正提案列表類
    if ly_common.GetLegislatorId(c, re.sub('\s', '', lines[-2])) or ly_common.GetLegislatorId(c, re.sub('\s', '', lines[-3])) or re.search(u'(案|審查)[\S]{0,3}$', lines[-2]):
        for i in range(base_line, 0-len(lines), -1):
            if re.search(u'^(附表|如下)\S{0,2}$', lines[i]):
                for j in range(i, 0-len(lines), -1):
                    if re.search(u'提議\W{0,4}(增列|報告事項|討論事項)', lines[j]):
                        return '\n'.join(lines[j:])
            if re.search(u'(通過|附表|如下)\S{1,2}$', lines[i]):
                return '\n'.join(lines[i:])
            if re.search(u'^增列報告事項$', lines[i]):
                return '\n'.join(lines[i:])
    # 提議增列列表類
    for i in range(base_line, 0-len(lines), -1):
        if re.search(u'提議\W{0,4}增列', lines[i]):
            return '\n'.join(lines[i:])
    # 剩下的先向上找上一個附後，找兩附後之間以冒號作結，如找不到
    if vote_seq != '001':
        for i in range(base_line, 0-len(lines), -1):
            if re.search(u'附後\S[\d]+\S', lines[i]):
                base_line = i
                break
        for line in reversed(range(base_line-1, -3)):
            if re.search(u'[：:]$', lines[line]):
                return '\n'.join(lines[line:])
    # 最後方法
    if re.search(u'^[\S]{1,5}在場委員', lines[-1]):
        return '\n'.join(lines[-2:])
    else:
        return lines[-1]
    print text
    print 'WTF!!!'
    raw_input()

def iterateVoter(sitting_dict, text, vote_id, decision):
    for legislator_uid in ly_common.GetLegislatorIdList(c, text.strip().split('\n')[0]):
        legislator_id = ly_common.GetLegislatorDetailId(c, legislator_uid, sitting_dict["ad"])
        vote_common.upsert_vote_legislator_vote(c, legislator_id, vote_id, decision)

def IterEachDecision(votertext, sitting_dict, vote_id):
    votertext = re.sub(u'(\w+)\r\n(\w+)', u'\g<1>\g<2>', votertext) # e.g. Kolasi ^M Yotaka=>KolasYotaka, EOL
    mapprove, mreject, mquit = re.search(u'[\s、]贊成[\S]*?者[:：][\d]+人', votertext), re.search(u'[\s、]反對[\S]*?者[:：][\d]+人', votertext), re.search(u'[\s、]棄權者[:：][\d]+人', votertext)
    mnext = re.search(u'[\s、]贊成[\S]*?者[:：][\d]+人', votertext[mapprove.end():])
    if not mapprove:
        print u'==找不到贊成者==\n', votertext
        raw_input()
    else:
        target_list = re.sub(u'\n', u'　　', votertext[mapprove.end():mreject.start()])
        iterateVoter(sitting_dict, target_list, vote_id, 1)
    if not mreject:
        print u'==找不到反對者==\n', votertext
        raw_input()
    else:
        target_list = re.sub(u'\n', u'　　', votertext[mreject.end():mquit.start()])
        iterateVoter(sitting_dict, target_list, vote_id, -1)
    if not mquit:
        print u'==找不到棄權者==\n', votertext
    else:
        if mnext:
            target_list = re.sub(u'\n', u'　　', votertext[mquit.end():mapprove.end()+mnext.start()])
        else:
            target_list = re.sub(u'\n', u'　　', votertext[mquit.end():])
        iterateVoter(sitting_dict, target_list, vote_id, 0)
    return mapprove, mreject, mquit

def IterVote(text, sitting_dict):
    sitting_id = sitting_dict["uid"]
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
            print vote_id
            print vote_id
            content = GetVoteContent(vote_seq, text[:match.start()+2])
            category = u'變更議程順序' if re.search(u'提議(變更議程|\W{0,4}增列)', content.split('\n')[0]) else ''
            if content:
                vote_common.upsert_vote(c, vote_id, sitting_id, vote_seq, category, content)
                mapprove, mreject, mquit = IterEachDecision(votertext, sitting_dict, vote_id)
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
        content = GetVoteContent(vote_seq, text[:mvoter.start()])
        category = u''
        if content:
            vote_common.upsert_vote(c, vote_id, sitting_id, vote_seq, category, content)
        if vote_id:
            mapprove, mreject, mquit = IterEachDecision(votertext, sitting_dict, vote_id)

conn = db_settings.con()
c = conn.cursor()
ad = 9
sitting_ids = vote_common.sittingIdsInAd(c, ad)
dicts = json.load(open('vote/minutes.json'))
for meeting in reversed(dicts):
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
    if int(ms.group('ad')) != ad:
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
    ly_common.Attendance(c, sitting_dict, sourcetext, u'出席委員[:：　]?', 'YS', 'present')
    ly_common.Attendance(c, sitting_dict, sourcetext, u'請假委員[:：　]?', 'YS', 'absent')
    IterVote(sourcetext, sitting_dict)

vote_common.conscience_vote(c, ad)
vote_common.not_voting_and_results(c)
vote_common.vote_param(c)
vote_common.attendance_param(c)
conn.commit()
