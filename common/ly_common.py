# -*- coding: utf-8 -*-
import re
import json
import time
from datetime import datetime


def normalize_person_name(name):
    name = re.sub(u'[。˙・･•．.]', u'‧', name)
    name = re.sub(u'[　\s()（）]', '',name)
    name = name.title()
    return name

def normalize_person(person):
    person['name'] = re.sub(u'[。˙・･•．.]', u'‧', person['name'])
    person['name'] = re.sub(u'[　\s()（）]', '', person['name'])
    person['name'] = person['name'].title()
    for wrong, right in [(u'^江啓臣$', u'江啟臣')]:
        person['name'] = re.sub(wrong, right, person['name'])
    person['gender'] = re.sub(u'性', '', person.get('gender', ''))
    for key in ['party', 'elected_party', 'caucus']:
        if person.get(key):
            person[key] = person[key].strip()
            person[key] = re.sub(u'無黨?$', u'無黨籍', person[key]) #無,無黨
            person[key] = re.sub(u'無黨籍.*', u'無黨籍', person[key]) #無黨籍及未經政黨推薦
            person[key] = re.sub(u'台灣', u'臺灣', person[key])
            person[key] = re.sub(u'台聯黨', u'臺灣團結聯盟', person[key])
            person[key] = re.sub(u'^國民黨$', u'中國國民黨', person[key])
            person[key] = re.sub(u'^民進黨$', u'民主進步黨', person[key])
    return person

def SittingsAbbreviation(key):
    d = json.load(open('util.json'))
    return d.get(key)

def FileLog(c, sitting):
    c.execute('''
        INSERT into legislator_filelog(sitting, date)
        SELECT %s, %s
        WHERE NOT EXISTS (SELECT 1 FROM legislator_filelog WHERE sitting = %s) RETURNING id
    ''', (sitting, datetime.now(), sitting))

def GetDate(text):
    matchTerm = re.search(u'''
        (?P<year>[\d]+)[\s]*年[\s]*
        (?P<month>[\d]+)[\s]*月[\s]*
        (?P<day>[\d]+)
    ''', text, re.X)
    if matchTerm:
        return '%04d-%02d-%02d' % (int(matchTerm.group('year'))+1911, int(matchTerm.group('month')), int(matchTerm.group('day')))
    else:
        return None

def GetLegislatorId(c, name):
    identifiers = {name, re.sub(u'[\w‧]', '', name), re.sub(u'\W', '', name).lower(), } - {''}
    c.execute('''
        SELECT uid
        FROM legislator_legislator
        WHERE identifiers ?| array[%s]
    ''' % ','.join(["'%s'" % x for x in identifiers]))
    return c.fetchall()

def GetLegislatorDetailId(c, legislator_id, ad):
    if type(legislator_id) != type([]):
        legislator_id = [legislator_id]
    c.execute('''
        SELECT id
        FROM legislator_legislatordetail
        WHERE legislator_id in %s and ad = %s
    ''', (tuple(legislator_id), ad))
    r = c.fetchone()
    if r:
        return r[0]
    #print legislator_id

def GetLegislatorIdList(c, text):
    id_list = []
    text = text.strip(u'[　\s]')
    text = re.sub(u'([^　\w])　([^　\w])　', u'\g<1>\g<2>　', text) # e.g. 楊　曜=>楊曜, 包含句首
    text = re.sub(u'　([^　\w])　([^　\w])', u'　\g<1>\g<2>', text) # e.g. 楊　曜=>楊曜, 包含句尾
    text = re.sub(u'(\w+) (\w+)　', u'\g<1>\g<2>　', text) # e.g. Kolas Yotaka=>KolasYotaka, 包含句首
    text = re.sub(u'　(\w+) (\w+)', u'　\g<1>\g<2>', text) # e.g. Kolas Yotaka=>KolasYotaka, 包含句尾
    text = re.sub(u'^([^　\w])　([^　\w])$', u'\g<1>\g<2>', text) # e.g. 楊　曜=>楊曜, 單獨一人
    text = re.sub(u'^(\w+) (\w+)$', u'\g<1>\g<2>', text) # e.g. Kolas Yotaka=>KolasYotaka, 單獨一人
    for name in text.split():
        name = re.sub(u'(.*)[）)。】」]$', '\g<1>', name)   # 立委名字後有標點符號
        legislator_id = GetLegislatorId(c, name)
        if legislator_id:
            id_list.append(legislator_id)
        else:
            print '%s not an legislator?' % name
    return id_list

def AddAttendanceRecord(c, legislator_id, sitting_id, category, status):
    c.execute('''
        INSERT INTO legislator_attendance(legislator_id, sitting_id, category, status)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (legislator_id, sitting_id, category)
        DO UPDATE
        SET status = %s
    ''', (legislator_id, sitting_id, category, status, status))

def Attendance(c, sitting_dict, text, keyword, category, status):
    match = re.search(keyword, text)
    if match:
        for legislator_id in GetLegislatorIdList(c, text[match.end():].split('\n')[0]):
            legislator_id = GetLegislatorDetailId(c, legislator_id, sitting_dict["ad"])
            AddAttendanceRecord(c, legislator_id, sitting_dict["uid"], category, status)
    elif status == 'present':
        print "Can' find attendance list"
        raise

def InsertSitting(c, sitting_dict):
    complement = {"committee":'', "name":''}
    complement.update(sitting_dict)
    c.execute('''
        INSERT into sittings_sittings(uid, name, date, ad, session, committee, links)
        VALUES (%(uid)s, %(name)s, %(date)s, %(ad)s, %(session)s, %(committee)s, %(links)s)
        ON CONFLICT (uid)
        DO UPDATE
        SET name = %(name)s, date = %(date)s, ad = %(ad)s, session = %(session)s, committee = %(committee)s, links = %(links)s
    ''', complement)

def SittingDict(text):
    ms = re.search(u'''
        第(?P<ad>[\d]+)\s?屆
        第(?P<session>[\d]+)\s?會期
        第(?P<times>[\d]+)\s?次
        (臨時會第(?P<temptimes>[\d]+)\s?次)?
        (?:會議|全院委員會)
    ''', text, re.X)
    if ms.group('temptimes'):
        uid = '%02d-%02dT%02d-YS-%02d' % (int(ms.group('ad')), int(ms.group('session')), int(ms.group('times')), int(ms.group('temptimes')))
    else:
        uid = '%02d-%02d-YS-%02d' % (int(ms.group('ad')), int(ms.group('session')), int(ms.group('times')))
    return ms, uid
