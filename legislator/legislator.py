#! /usr/bin/env python
# -*- coding: utf-8 -*-
import re
import uuid
import codecs
import psycopg2
from psycopg2.extras import Json
import json

from common import ly_common
from common import db_settings


def normalize_constituency(constituency):
    match = re.search(u'第(?P<num>.+)選(?:舉)?區', constituency)
    if not match:
        return 1
    try:
        return int(match.group('num'))
    except:
        print match.group('num')
    ref = {u'一': 1, u'二': 2, u'三': 3, u'四': 4, u'五': 5, u'六': 6, u'七': 7, u'八': 8, u'九': 9}
    if re.search(u'^\s*十\s*$', match.group('num')):
        return 10
    num = re.sub(u'^\s*十', u'一', match.group('num'))
    num = re.sub(u'十', '', num)
    digits = re.findall(u'(一|二|三|四|五|六|七|八|九)', num)
    total, dec = 0, 1
    for i in reversed(range(0, len(digits))):
        total = total + int(ref.get(digits[i], 0)) * dec
        dec = dec * 10
    return total

def Legislator(legislator):
    if legislator.has_key('former_names'):
        legislator['former_names'] = '\n'.join(legislator['former_names'])
    else:
        legislator.update({'former_names': ''})
    c.execute('''
        UPDATE legislator_legislator
        SET name = %(name)s, former_names = %(former_names)s
        WHERE uid = %(uid)s
    ''', legislator)
    c.execute('''
        INSERT INTO legislator_legislator(uid, name, former_names)
        SELECT %(uid)s, %(name)s, %(former_names)s
        WHERE NOT EXISTS (SELECT 1 FROM legislator_legislator WHERE uid = %(uid)s)
    ''', legislator)

def LegislatorDetail(uid, term, ideal_term_end_year):
    for key in ['education', 'experience', 'remark']:
        if term.has_key(key):
            term[key] = '\n'.join(term[key])
    term.pop('county', None)
    if term.has_key('district'):
        term['district'] = u'，'.join([x for x in term['district']])
    term['term_end'] = {"date": '%04d-01-31' % int(ideal_term_end_year)}
    complement = {"uid": uid, "gender": '', "party": '', "caucus": '', "contacts": None, "county": term['constituency'], "constituency": 0, "district": '', "term_start": None, "education": None, "experience": None, "remark": None, "image": '', "links": None}
    match = re.search(u'(?P<county>[\W]{1,2}(縣|市))', term['constituency'])
    if match:
        complement.update({"county": re.sub(u'台', u'臺', match.group('county'))})
    if term.get('constituency'):
        term['constituency'] = normalize_constituency(term['constituency'])
    complement.update(term)
    complement['party'] = [{"name": complement['party'], "end_at": complement['term_end']['date'], "start_at": complement['term_start']}]
    c.execute('''
        UPDATE legislator_legislatordetail
        SET name = %(name)s, gender = %(gender)s, title = %(title)s, party = %(party)s, elected_party = %(elected_party)s, caucus = %(caucus)s, constituency = %(constituency)s, in_office = %(in_office)s, contacts = %(contacts)s, county = %(county)s, district = %(district)s, term_start = %(term_start)s, term_end = %(term_end)s, education = %(education)s, experience = %(experience)s, remark = %(remark)s, image = %(image)s, links = %(links)s
        WHERE legislator_id = %(uid)s AND ad = %(ad)s
    ''', complement)
    c.execute('''
        INSERT into legislator_legislatordetail(legislator_id, ad, name, gender, title, party, elected_party, caucus, constituency, county, district, in_office, contacts, term_start, term_end, education, experience, remark, image, links)
        SELECT %(uid)s, %(ad)s, %(name)s, %(gender)s, %(title)s, %(party)s, %(elected_party)s, %(caucus)s, %(constituency)s, %(county)s, %(district)s, %(in_office)s, %(contacts)s, %(term_start)s, %(term_end)s, %(education)s, %(experience)s, %(remark)s, %(image)s, %(links)s
        WHERE NOT EXISTS (SELECT 1 FROM legislator_legislatordetail WHERE legislator_id = %(uid)s AND ad = %(ad)s)
    ''', complement)

def Committees(committees):
    c.executemany('''
        INSERT INTO committees_committees(name)
        SELECT %(name)s
        WHERE NOT EXISTS (SELECT 1 FROM committees_committees WHERE name = %(name)s)
    ''', committees)

def Legislator_Committees(legislator_id, committee):
    complement = {"legislator_id":legislator_id}
    complement.update(committee)
    c.execute('''
        UPDATE committees_legislator_committees
        SET chair = %(chair)s
        WHERE legislator_id = %(legislator_id)s AND committee_id = %(name)s AND ad = %(ad)s AND session = %(session)s
    ''', complement)
    c.execute('''
        INSERT INTO committees_legislator_committees(legislator_id, committee_id, ad, session, chair)
        SELECT %(legislator_id)s, %(name)s, %(ad)s, %(session)s, %(chair)s
        WHERE NOT EXISTS (SELECT 1 FROM committees_legislator_committees WHERE legislator_id = %(legislator_id)s AND committee_id = %(name)s AND ad = %(ad)s AND session = %(session)s )
    ''', complement)

conn = db_settings.con()
c = conn.cursor()

f = codecs.open('legislator/no_committees.txt', 'w', encoding='utf-8')
dict_list = json.load(open('data/twly_crawler/data/merged.json'))
ideal_term_end_year = {'1': 1993, '2': 1996, '3': 1999, '4': 2002, '5': 2005, '6': 2008, '7': 2012, '8': 2016, '9': 2020}
for legislator in dict_list:
    legislator = ly_common.normalize_person(legislator)
    Legislator(legislator)
    for term in legislator["each_term"]:
        term = ly_common.normalize_person(term)
        LegislatorDetail(legislator["uid"], term, ideal_term_end_year[str(term["ad"])])
        legislator_id = ly_common.GetLegislatorDetailId(c, legislator["uid"], term["ad"])
        if term.has_key("committees"):
            Committees(term["committees"])
            for committee in term["committees"]:
                Legislator_Committees(legislator_id, committee)
        else:
            f.write('no committees!!, uid: %s, name: %s, ad: %s\n' % (legislator["uid"], term["name"], term["ad"]))
f.close()
conn.commit()
party_change = json.load(open('legislator/party_change.json'))
for instance in party_change:
    c.execute('''
        UPDATE legislator_legislatordetail
        SET party = %(party)s
        WHERE ad = %(ad)s AND name = %(name)s
    ''', instance)
conn.commit()

